use std::{sync::Arc, time::Duration};

use futures_util::{SinkExt, StreamExt};
use quilr_remote_connector::{
    forwarder::HttpForwarder,
    protocol::{ConnectorToGatewayMessage, GatewayToConnectorMessage},
    tunnel::{MAX_ACTIVE_STREAMS, TunnelConfig, run_single_session},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;

fn config() -> TunnelConfig {
    TunnelConfig {
        gateway_ws_url: None,
        token: "qrc_test".into(),
        instance_name: None,
        insecure_skip_tls_verify: false,
        ping_interval: Duration::from_secs(20),
    }
}

fn request(stream_id: String, url: String) -> Message {
    Message::Text(
        serde_json::to_string(&GatewayToConnectorMessage::HttpRequest {
            stream_id,
            method: "GET".into(),
            url,
            headers: vec![],
            body: Vec::new(),
            body_b64: None,
            timeout_ms: Some(0),
        })
        .unwrap(),
    )
}

#[tokio::test]
async fn shutdown_cancels_a_stalled_websocket_handshake() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (accepted_tx, accepted_rx) = oneshot::channel();
    let stalled = tokio::spawn(async move {
        let (_socket, _) = listener.accept().await.unwrap();
        accepted_tx.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    let shutdown = CancellationToken::new();
    let token = shutdown.clone();
    let connector = tokio::spawn(async move {
        run_single_session(
            &format!("ws://{addr}"),
            &config(),
            Arc::new(HttpForwarder::new(false).unwrap()),
            token,
        )
        .await
    });
    accepted_rx.await.unwrap();
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(1), connector)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    stalled.abort();
}

#[tokio::test]
async fn websocket_handshake_timeout_closes_the_blackholed_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (accepted_tx, accepted_rx) = oneshot::channel();
    let stalled = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        loop {
            let mut byte = [0];
            socket.read_exact(&mut byte).await.unwrap();
            request.extend(byte);
            if request.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        accepted_tx.send(()).unwrap();
        // TCP succeeded, but the gateway never sends its HTTP Upgrade response.
        let mut byte = [0];
        assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
    });
    let connector = tokio::spawn(async move {
        run_single_session(
            &format!("ws://{addr}"),
            &config(),
            Arc::new(HttpForwarder::new(false).unwrap()),
            CancellationToken::new(),
        )
        .await
    });
    accepted_rx.await.unwrap();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(31)).await;
    let error = connector.await.unwrap().unwrap_err().to_string();
    assert!(error.contains("connection timed out"), "{error}");
    stalled.await.unwrap();
}

#[tokio::test]
async fn duplicate_active_stream_id_closes_session_instead_of_orphaning_original() {
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = format!("http://{}/", upstream.local_addr().unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let gateway = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        assert!(matches!(ws.next().await, Some(Ok(Message::Text(_)))));
        ws.send(request("duplicate".into(), target.clone()))
            .await
            .unwrap();
        ws.send(request("duplicate".into(), target)).await.unwrap();
        while let Some(Ok(message)) = ws.next().await {
            if matches!(message, Message::Close(_)) {
                break;
            }
        }
    });
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        run_single_session(
            &format!("ws://{addr}"),
            &config(),
            Arc::new(HttpForwarder::new(false).unwrap()),
            CancellationToken::new(),
        ),
    )
    .await
    .unwrap();
    assert!(result.unwrap_err().to_string().contains("duplicate"));
    gateway.await.unwrap();
}

#[tokio::test]
async fn active_stream_count_is_bounded_and_capacity_failure_uses_wire_error() {
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = format!("http://{}/", upstream.local_addr().unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let gateway = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        ws.next().await.unwrap().unwrap();
        for n in 0..=MAX_ACTIVE_STREAMS {
            ws.send(request(n.to_string(), target.clone()))
                .await
                .unwrap();
        }
        loop {
            if let Some(Ok(Message::Text(json))) = ws.next().await {
                match serde_json::from_str::<ConnectorToGatewayMessage>(&json).unwrap() {
                    ConnectorToGatewayMessage::HttpResponseError { stream_id, error } => {
                        assert_eq!(stream_id, MAX_ACTIVE_STREAMS.to_string());
                        assert!(error.contains("capacity"));
                        break;
                    }
                    ConnectorToGatewayMessage::Ping { .. } => {}
                    other => panic!("Unexpected frame {other:?}"),
                }
            } else {
                panic!("Session closed without capacity error");
            }
        }
        ws.close(None).await.unwrap();
    });
    tokio::time::timeout(
        Duration::from_secs(5),
        run_single_session(
            &format!("ws://{addr}"),
            &config(),
            Arc::new(HttpForwarder::new(false).unwrap()),
            CancellationToken::new(),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    gateway.await.unwrap();
}

async fn upstream_stream() -> (String, oneshot::Receiver<()>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/", listener.local_addr().unwrap());
    let (closed_tx, closed_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        loop {
            let mut byte = [0];
            socket.read_exact(&mut byte).await.unwrap();
            request.extend(byte);
            if request.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n5\r\ndata:\r\n").await.unwrap();
        let mut byte = [0];
        assert_eq!(
            socket.read(&mut byte).await.unwrap(),
            0,
            "Upstream socket remains open"
        );
        closed_tx.send(()).unwrap();
    });
    (url, closed_rx, task)
}

#[tokio::test]
async fn remote_cancel_and_disconnect_both_release_upstream_streams() {
    for send_cancel in [true, false] {
        let (target, upstream_closed, upstream_task) = upstream_stream().await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (ready_tx, ready_rx) = oneshot::channel();
        let gateway = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(socket).await.unwrap();
            ws.next().await.unwrap().unwrap();
            ws.send(request("stream".into(), target)).await.unwrap();
            loop {
                if let Some(Ok(Message::Text(json))) = ws.next().await
                    && matches!(
                        serde_json::from_str::<ConnectorToGatewayMessage>(&json).unwrap(),
                        ConnectorToGatewayMessage::HttpResponseStart { .. }
                    )
                {
                    break;
                }
            }
            if send_cancel {
                ws.send(Message::Text(
                    serde_json::to_string(&GatewayToConnectorMessage::CancelStream {
                        stream_id: "stream".into(),
                    })
                    .unwrap(),
                ))
                .await
                .unwrap();
                ready_tx.send(()).unwrap();
                // Keep the tunnel open; cancellation alone must close the upstream socket.
                while ws.next().await.is_some() {}
            } else {
                ws.close(None).await.unwrap();
                ready_tx.send(()).unwrap();
            }
        });
        let shutdown = CancellationToken::new();
        let token = shutdown.clone();
        let connector = tokio::spawn(async move {
            run_single_session(
                &format!("ws://{addr}"),
                &config(),
                Arc::new(HttpForwarder::new(false).unwrap()),
                token,
            )
            .await
        });
        ready_rx.await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), upstream_closed)
            .await
            .unwrap()
            .unwrap();
        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(2), connector)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        gateway.abort();
        upstream_task.await.unwrap();
    }
}

#[test]
fn gateway_endpoint_normalization_matches_python_and_rejects_credentials() {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    for (input, expected) in [
        (
            "https://gateway.example/proxy/ws?token=secret#frag",
            "wss://gateway.example/proxy/ws/api/remote-connectors/ws",
        ),
        (
            "https://gateway.example/proxy/api/remote-connectors/ws/?x=1#frag",
            "wss://gateway.example/proxy/api/remote-connectors/ws",
        ),
        (
            "http://gateway.example/proxy/",
            "ws://gateway.example/proxy/api/remote-connectors/ws",
        ),
        (
            "ws://gateway.example/",
            "ws://gateway.example/api/remote-connectors/ws",
        ),
    ] {
        let mut cfg = config();
        cfg.gateway_ws_url = Some(input.into());
        assert_eq!(cfg.normalized_ws_url().unwrap(), expected);
        cfg.gateway_ws_url = None;
        cfg.token = format!("qrc_{}.secret", URL_SAFE_NO_PAD.encode(input));
        assert_eq!(cfg.normalized_ws_url().unwrap(), expected);
    }
    for input in [
        "https://user:secret@gateway.example",
        "https://user@gateway.example",
        "ftp://gateway.example",
        "https://",
    ] {
        let mut cfg = config();
        cfg.gateway_ws_url = Some(input.into());
        let error = cfg.normalized_ws_url().unwrap_err().to_string();
        assert!(!error.contains("secret"));
    }
}

#[tokio::test]
async fn aborting_session_future_also_stops_writer_and_upstream_tasks() {
    let (target, upstream_closed, upstream_task) = upstream_stream().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (ready_tx, ready_rx) = oneshot::channel();
    let gateway = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        ws.next().await.unwrap().unwrap();
        ws.send(request("aborted".into(), target)).await.unwrap();
        loop {
            let Some(Ok(Message::Text(json))) = ws.next().await else {
                panic!("Missing headers");
            };
            if matches!(
                serde_json::from_str::<ConnectorToGatewayMessage>(&json).unwrap(),
                ConnectorToGatewayMessage::HttpResponseStart { .. }
            ) {
                break;
            }
        }
        ready_tx.send(()).unwrap();
        while let Some(Ok(message)) = ws.next().await {
            if matches!(message, Message::Close(_)) {
                return;
            }
        }
    });
    let connector = tokio::spawn(async move {
        run_single_session(
            &format!("ws://{addr}"),
            &config(),
            Arc::new(HttpForwarder::new(false).unwrap()),
            CancellationToken::new(),
        )
        .await
    });
    ready_rx.await.unwrap();
    connector.abort();
    assert!(connector.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(2), upstream_closed)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), gateway)
        .await
        .unwrap()
        .unwrap();
    upstream_task.await.unwrap();
}

#[tokio::test]
async fn silent_gateway_is_disconnected_after_heartbeat_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (ready_tx, ready_rx) = oneshot::channel();
    let gateway = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        ws.next().await.unwrap().unwrap();
        ready_tx.send(()).unwrap();
        // Read heartbeats without replying, simulating an unresponsive gateway application.
        while let Some(Ok(message)) = ws.next().await {
            if matches!(message, Message::Close(_)) {
                return;
            }
        }
    });
    let connector = tokio::spawn(async move {
        run_single_session(
            &format!("ws://{addr}"),
            &config(),
            Arc::new(HttpForwarder::new(false).unwrap()),
            CancellationToken::new(),
        )
        .await
    });
    ready_rx.await.unwrap();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(91)).await;
    let error = connector.await.unwrap().unwrap_err().to_string();
    assert!(error.contains("heartbeat timed out"));
    gateway.await.unwrap();
}

#[tokio::test]
async fn completed_stream_churn_reaps_tasks_instead_of_exhausting_active_capacity() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let gateway = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        ws.next().await.unwrap().unwrap();
        for n in 0..MAX_ACTIVE_STREAMS * 16 {
            let expected_id = n.to_string();
            ws.send(request(expected_id.clone(), "invalid-url".into()))
                .await
                .unwrap();
            loop {
                let Some(Ok(Message::Text(json))) = ws.next().await else {
                    panic!("Session closed during stream churn");
                };
                match serde_json::from_str::<ConnectorToGatewayMessage>(&json).unwrap() {
                    ConnectorToGatewayMessage::HttpResponseError { stream_id, error } => {
                        assert_eq!(stream_id, expected_id);
                        assert!(
                            error.contains("Invalid target URL"),
                            "Capacity/task leak: {error}"
                        );
                        break;
                    }
                    ConnectorToGatewayMessage::Ping { .. } => {}
                    other => panic!("Unexpected frame {other:?}"),
                }
            }
        }
        ws.close(None).await.unwrap();
    });
    tokio::time::timeout(
        Duration::from_secs(15),
        run_single_session(
            &format!("ws://{addr}"),
            &config(),
            Arc::new(HttpForwarder::new(false).unwrap()),
            CancellationToken::new(),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    gateway.await.unwrap();
}

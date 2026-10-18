use std::time::Duration;

use clap::Parser;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::EnvFilter;

use quilr_remote_connector::tunnel::{TunnelConfig, run_connector_forever};

#[derive(Parser, Debug)]
#[command(
    name = "quilr-connector",
    version,
    about = "Quilr Remote Connector — connects private cluster/local MCP servers and REST APIs (URL-to-MCP) to Quilr MCP Gateway over WebSocket"
)]
struct Cli {
    /// Connector authentication token generated in the Quilr Admin Console (qrc_<encoded_gateway_url>.<secret>)
    #[arg(long, env = "QUILR_CONNECTOR_TOKEN", hide_env_values = true)]
    token: String,

    /// Optional Quilr MCP Gateway base URL override (if omitted, automatically decoded from --token)
    #[arg(long, env = "QUILR_GATEWAY_URL")]
    gateway_url: Option<String>,

    /// Optional instance or pod label reported to the Admin Console (defaults to hostname)
    #[arg(long, env = "QUILR_CONNECTOR_INSTANCE_NAME")]
    instance_name: Option<String>,

    /// Skip TLS certificate verification when connecting to local/internal HTTPS MCP servers
    #[arg(long, env = "QUILR_CONNECTOR_INSECURE_TLS", default_value_t = false)]
    insecure_skip_tls_verify: bool,

    /// Heartbeat ping interval in seconds
    #[arg(long, env = "QUILR_CONNECTOR_PING_INTERVAL_SECS", default_value_t = 20)]
    ping_interval_secs: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let cli = Cli::parse();

    let config = TunnelConfig {
        gateway_ws_url: cli.gateway_url,
        token: cli.token,
        instance_name: cli.instance_name,
        insecure_skip_tls_verify: cli.insecure_skip_tls_verify,
        ping_interval: Duration::from_secs(cli.ping_interval_secs.max(5)),
    };

    let shutdown = CancellationToken::new();
    let shutdown_signal = shutdown.clone();

    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let mut sigterm = signal(SignalKind::terminate()).expect("register SIGTERM");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    info!("Received SIGINT, shutting down connector...");
                }
                _ = sigterm.recv() => {
                    info!("Received SIGTERM, shutting down connector...");
                }
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
            info!("Received Ctrl+C, shutting down connector...");
        }
        shutdown_signal.cancel();
    });

    run_connector_forever(config, shutdown).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Router,
        extract::{
            State,
            ws::{Message as AxumWsMessage, WebSocket, WebSocketUpgrade},
        },
        routing::{get, post},
    };
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use futures_util::StreamExt;
    use quilr_remote_connector::protocol::{ConnectorToGatewayMessage, GatewayToConnectorMessage};
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;

    type TunnelResponses = (u16, String, u16, String);
    type SharedResponses = Arc<tokio::sync::Mutex<Option<oneshot::Sender<TunnelResponses>>>>;
    type GatewayState = (std::net::SocketAddr, SharedResponses);

    #[test]
    fn test_normalized_ws_url_and_token_decoding() {
        let encoded_url = URL_SAFE_NO_PAD.encode("https://mcpgateway.quilr.ai");
        let self_contained_token = format!("qrc_{encoded_url}.abcdef0123456789");

        let cfg_from_token = TunnelConfig {
            gateway_ws_url: None,
            token: self_contained_token,
            instance_name: None,
            insecure_skip_tls_verify: false,
            ping_interval: Duration::from_secs(20),
        };
        assert_eq!(
            cfg_from_token.normalized_ws_url().unwrap(),
            "wss://mcpgateway.quilr.ai/api/remote-connectors/ws"
        );

        let cfg_explicit = TunnelConfig {
            gateway_ws_url: Some("http://127.0.0.1:8005/".to_string()),
            token: "qrc_test".to_string(),
            instance_name: None,
            insecure_skip_tls_verify: false,
            ping_interval: Duration::from_secs(20),
        };
        assert_eq!(
            cfg_explicit.normalized_ws_url().unwrap(),
            "ws://127.0.0.1:8005/api/remote-connectors/ws"
        );
    }

    #[tokio::test]
    async fn test_end_to_end_http_and_sse_forwarding() {
        // 1. Start a mock local MCP server
        let local_mcp_app = Router::new()
            .route(
                "/mcp",
                post(|body: String| async move {
                    (
                        [
                            ("content-type", "application/json"),
                            ("mcp-session-id", "sess-123"),
                        ],
                        format!("{{\"echo\":{body}}}"),
                    )
                }),
            )
            .route(
                "/sse",
                get(|| async move {
                    (
                        [("content-type", "text/event-stream")],
                        "event: message\ndata: {\"jsonrpc\":\"2.0\",\"result\":\"ok\"}\n\n",
                    )
                }),
            );

        let mcp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mcp_addr = mcp_listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(mcp_listener, local_mcp_app).await.unwrap();
        });

        // 2. Start a mock Gateway WebSocket server that verifies Hello, sends a POST /mcp
        //    and a GET /sse through the tunnel, and signals completion.
        let (done_tx, done_rx) = oneshot::channel::<(u16, String, u16, String)>();
        let shared_done = Arc::new(tokio::sync::Mutex::new(Some(done_tx)));

        let gw_state = (mcp_addr, shared_done.clone());
        let gateway_app = Router::new()
            .route(
                "/api/remote-connectors/ws",
                get(
                    |ws: WebSocketUpgrade,
                     State((mcp_addr, done_tx)): State<GatewayState>| async move {
                        ws.on_upgrade(move |socket| {
                            handle_mock_gateway_ws(socket, mcp_addr, done_tx)
                        })
                    },
                ),
            )
            .with_state(gw_state);

        let gw_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let gw_addr = gw_listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(gw_listener, gateway_app).await.unwrap();
        });

        // 3. Run the connector against the mock gateway using a self-contained token (no gateway_ws_url!)
        let shutdown = CancellationToken::new();
        let connector_shutdown = shutdown.clone();
        let encoded_gw = URL_SAFE_NO_PAD.encode(format!("http://{gw_addr}"));
        let token = format!("qrc_{encoded_gw}.secret123456");
        let config = TunnelConfig {
            gateway_ws_url: None,
            token,
            instance_name: Some("test-pod-1".to_string()),
            insecure_skip_tls_verify: false,
            ping_interval: Duration::from_secs(10),
        };

        let connector_task = tokio::spawn(async move {
            let _ = run_connector_forever(config, connector_shutdown).await;
        });

        let (status1, body1, status2, body2) =
            tokio::time::timeout(Duration::from_secs(5), done_rx)
                .await
                .expect("timed out waiting for tunnel responses")
                .unwrap();

        assert_eq!(status1, 200);
        assert_eq!(body1, "{\"echo\":{\"method\":\"initialize\"}}");
        assert_eq!(status2, 200);
        assert!(body2.contains("event: message"));

        shutdown.cancel();
        let _ = connector_task.await;
    }

    async fn handle_mock_gateway_ws(
        mut socket: WebSocket,
        mcp_addr: std::net::SocketAddr,
        done_tx: SharedResponses,
    ) {
        // Expect Hello first
        let first = socket.next().await.unwrap().unwrap();
        if let AxumWsMessage::Text(txt) = first {
            let parsed: ConnectorToGatewayMessage = serde_json::from_str(&txt).unwrap();
            match parsed {
                ConnectorToGatewayMessage::Hello { instance_name, .. } => {
                    assert_eq!(instance_name.as_deref(), Some("test-pod-1"));
                }
                other => panic!("Expected Hello, got {other:?}"),
            }
        }

        // Send POST /mcp request as a binary frame (<json_header>\n<raw_body>)
        let req1 = GatewayToConnectorMessage::HttpRequest {
            stream_id: "stream-1".to_string(),
            method: "POST".to_string(),
            url: format!("http://{mcp_addr}/mcp"),
            headers: vec![("content-type".to_string(), "application/json".to_string())],
            body: Vec::new(),
            body_b64: None,
            timeout_ms: Some(5000),
        };
        let mut req1_frame = serde_json::to_vec(&req1).unwrap();
        req1_frame.push(b'\n');
        req1_frame.extend_from_slice(b"{\"method\":\"initialize\"}");
        socket
            .send(AxumWsMessage::Binary(req1_frame))
            .await
            .unwrap();

        let mut status1 = 0;
        let mut body1 = Vec::new();
        while let Some(Ok(frame)) = socket.next().await {
            match frame {
                AxumWsMessage::Text(txt) => {
                    let msg: ConnectorToGatewayMessage = serde_json::from_str(&txt).unwrap();
                    match msg {
                        ConnectorToGatewayMessage::HttpResponseStart { status, .. } => {
                            status1 = status;
                        }
                        ConnectorToGatewayMessage::HttpResponseEnd { .. } => break,
                        ConnectorToGatewayMessage::Ping { .. } => continue,
                        other => panic!("Unexpected text frame on stream-1: {other:?}"),
                    }
                }
                AxumWsMessage::Binary(bin) => {
                    let nl = bin.iter().position(|&b| b == b'\n').unwrap();
                    let header: serde_json::Value = serde_json::from_slice(&bin[..nl]).unwrap();
                    assert_eq!(header["type"], "http_response_chunk");
                    assert_eq!(header["stream_id"], "stream-1");
                    body1.extend_from_slice(&bin[nl + 1..]);
                }
                _ => continue,
            }
        }

        // Send GET /sse request
        let req2 = GatewayToConnectorMessage::HttpRequest {
            stream_id: "stream-2".to_string(),
            method: "GET".to_string(),
            url: format!("http://{mcp_addr}/sse"),
            headers: vec![("accept".to_string(), "text/event-stream".to_string())],
            body: Vec::new(),
            body_b64: None,
            timeout_ms: Some(5000),
        };
        socket
            .send(AxumWsMessage::Text(serde_json::to_string(&req2).unwrap()))
            .await
            .unwrap();

        let mut status2 = 0;
        let mut body2 = Vec::new();
        while let Some(Ok(frame)) = socket.next().await {
            match frame {
                AxumWsMessage::Text(txt) => {
                    let msg: ConnectorToGatewayMessage = serde_json::from_str(&txt).unwrap();
                    match msg {
                        ConnectorToGatewayMessage::HttpResponseStart { status, .. } => {
                            status2 = status;
                        }
                        ConnectorToGatewayMessage::HttpResponseEnd { .. } => break,
                        ConnectorToGatewayMessage::Ping { .. } => continue,
                        other => panic!("Unexpected text frame on stream-2: {other:?}"),
                    }
                }
                AxumWsMessage::Binary(bin) => {
                    let nl = bin.iter().position(|&b| b == b'\n').unwrap();
                    let header: serde_json::Value = serde_json::from_slice(&bin[..nl]).unwrap();
                    assert_eq!(header["type"], "http_response_chunk");
                    assert_eq!(header["stream_id"], "stream-2");
                    body2.extend_from_slice(&bin[nl + 1..]);
                }
                _ => continue,
            }
        }

        if let Some(tx) = done_tx.lock().await.take() {
            let _ = tx.send((
                status1,
                String::from_utf8(body1).unwrap(),
                status2,
                String::from_utf8(body2).unwrap(),
            ));
        }
    }
}

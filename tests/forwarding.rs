use std::convert::Infallible;
use std::time::{Duration, Instant};

use axum::{
    Router,
    body::Body,
    extract::DefaultBodyLimit,
    http::{HeaderMap, HeaderValue, Response},
    routing::{get, post},
};
use bytes::Bytes;
use futures_util::stream;
use quilr_remote_connector::{
    forwarder::{HttpForwarder, MAX_REQUEST_BODY_BYTES, RESPONSE_CHUNK_BYTES},
    protocol::ConnectorToGatewayMessage,
};
use tokio::{net::TcpListener, sync::mpsc};
use tokio_util::sync::CancellationToken;

async fn serve(app: Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}/"), server)
}

fn forward(
    url: String,
    headers: Vec<(String, String)>,
    timeout_ms: Option<u64>,
    tx: mpsc::Sender<ConnectorToGatewayMessage>,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        HttpForwarder::new(false)
            .unwrap()
            .execute_stream(
                "stream".into(),
                "GET".into(),
                url,
                headers,
                None,
                timeout_ms,
                tx,
                cancel,
            )
            .await;
    })
}

#[tokio::test]
async fn encoded_body_and_latin1_headers_survive_and_connection_headers_do_not() {
    let app = Router::new().route(
        "/",
        get(|headers: HeaderMap| async move {
            assert!(!headers.contains_key("x-private-hop"));
            assert!(!headers.contains_key("connection"));
            assert_eq!(headers["accept-encoding"], "custom");
            assert_eq!(headers["x-raw"].as_bytes(), b"caf\xe9");
            Response::builder()
                .status(200)
                .header("connection", "X-Response-Hop")
                .header("x-response-hop", "private")
                .header("content-encoding", "custom")
                .header("x-raw", HeaderValue::from_bytes(b"caf\xe9").unwrap())
                .body(Body::from(vec![0xff, 0x00, 0x7f]))
                .unwrap()
        }),
    );
    let (url, server) = serve(app).await;
    let (tx, mut rx) = mpsc::channel(8);
    let task = forward(
        url,
        vec![
            ("Connection".into(), "X-Private-Hop".into()),
            ("X-Private-Hop".into(), "private".into()),
            ("Accept-Encoding".into(), "custom".into()),
            ("X-Raw".into(), "caf\u{e9}".into()),
        ],
        Some(1000),
        tx,
        CancellationToken::new(),
    );
    let mut body = Vec::new();
    let mut saw_start = false;
    while let Some(message) = rx.recv().await {
        match message {
            ConnectorToGatewayMessage::HttpResponseStart {
                status, headers, ..
            } => {
                assert_eq!(status, 200);
                assert!(headers.contains(&("content-encoding".into(), "custom".into())));
                assert!(headers.contains(&("x-raw".into(), "caf\u{e9}".into())));
                assert!(!headers.iter().any(|(name, _)| {
                    ["connection", "x-response-hop", "content-length"].contains(&name.as_str())
                }));
                saw_start = true;
            }
            ConnectorToGatewayMessage::HttpResponseChunk { data, .. } => body.extend(data),
            ConnectorToGatewayMessage::HttpResponseEnd { .. } => break,
            other => panic!("Unexpected response: {other:?}"),
        }
    }
    task.await.unwrap();
    assert!(saw_start);
    assert_eq!(body, [0xff, 0x00, 0x7f]);
    server.abort();
}

#[tokio::test]
async fn slow_active_sse_outlives_idle_timeout_and_chunks_are_bounded() {
    let app = Router::new().route(
        "/",
        get(|| async {
            let chunks = stream::unfold(0, |n| async move {
                if n == 4 {
                    return None;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
                Some((
                    Ok::<_, Infallible>(Bytes::from(vec![b'x'; RESPONSE_CHUNK_BYTES * 3])),
                    n + 1,
                ))
            });
            Response::builder()
                .header("content-type", "text/event-stream")
                .body(Body::from_stream(chunks))
                .unwrap()
        }),
    );
    let (url, server) = serve(app).await;
    let (tx, mut rx) = mpsc::channel(8);
    let start = Instant::now();
    let task = forward(url, vec![], Some(150), tx, CancellationToken::new());
    let mut total = 0;
    let mut ended = false;
    while let Some(message) = rx.recv().await {
        match message {
            ConnectorToGatewayMessage::HttpResponseStart { .. } => {}
            ConnectorToGatewayMessage::HttpResponseChunk { data, .. } => {
                assert!(data.len() <= RESPONSE_CHUNK_BYTES);
                total += data.len();
            }
            ConnectorToGatewayMessage::HttpResponseEnd { .. } => {
                ended = true;
                break;
            }
            other => panic!("Unexpected response: {other:?}"),
        }
    }
    task.await.unwrap();
    assert!(ended);
    assert_eq!(total, RESPONSE_CHUNK_BYTES * 12);
    assert!(start.elapsed() > Duration::from_millis(150));
    server.abort();
}

#[tokio::test]
async fn idle_body_times_out_and_zero_timeout_waits_for_cancellation() {
    let app = Router::new().route(
        "/",
        get(|| async { Body::from_stream(stream::pending::<Result<Bytes, Infallible>>()) }),
    );
    let (url, server) = serve(app).await;
    let (tx, mut rx) = mpsc::channel(8);
    let task = forward(url.clone(), vec![], Some(40), tx, CancellationToken::new());
    assert!(matches!(
        rx.recv().await,
        Some(ConnectorToGatewayMessage::HttpResponseStart { .. })
    ));
    assert!(
        matches!(rx.recv().await, Some(ConnectorToGatewayMessage::HttpResponseError { error, .. }) if error.contains("idle timeout"))
    );
    task.await.unwrap();

    let (tx, mut rx) = mpsc::channel(8);
    let cancel = CancellationToken::new();
    let task = forward(url, vec![], Some(0), tx, cancel.clone());
    assert!(matches!(
        rx.recv().await,
        Some(ConnectorToGatewayMessage::HttpResponseStart { .. })
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(70), rx.recv())
            .await
            .is_err()
    );
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    assert!(rx.recv().await.is_none());
    server.abort();
}

#[tokio::test]
async fn cancellation_interrupts_full_outbound_queue_including_error_reporting() {
    // Invalid URL reports an error; its queue wait must still observe cancellation.
    let (tx, _rx) = mpsc::channel(1);
    tx.send(ConnectorToGatewayMessage::Pong { ts: None })
        .await
        .unwrap();
    let cancel = CancellationToken::new();
    let task = forward("invalid-url".into(), vec![], None, tx, cancel.clone());
    tokio::task::yield_now().await;
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();

    // A real response blocked on its first headers frame must also release the body.
    let (url, server) = serve(Router::new().route("/", get(|| async { "body" }))).await;
    let (tx, _rx) = mpsc::channel(1);
    tx.send(ConnectorToGatewayMessage::Pong { ts: None })
        .await
        .unwrap();
    let cancel = CancellationToken::new();
    let task = forward(url, vec![], None, tx, cancel.clone());
    tokio::time::sleep(Duration::from_millis(20)).await;
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    server.abort();
}

#[tokio::test]
async fn oversized_request_is_rejected_before_network_io() {
    use quilr_remote_connector::forwarder::MAX_REQUEST_BODY_BYTES;
    let (tx, mut rx) = mpsc::channel(1);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/", listener.local_addr().unwrap());
    HttpForwarder::new(false)
        .unwrap()
        .execute_stream(
            "oversize".into(),
            "POST".into(),
            url,
            vec![],
            Some(vec![b'A'; MAX_REQUEST_BODY_BYTES + 1]),
            None,
            tx,
            CancellationToken::new(),
        )
        .await;
    assert!(
        matches!(rx.recv().await, Some(ConnectorToGatewayMessage::HttpResponseError { error, .. }) if error.contains("maximum size"))
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn sixteen_mib_request_and_response_roundtrip_without_truncation() {
    assert_eq!(MAX_REQUEST_BODY_BYTES, 16 * 1024 * 1024);
    let app = Router::new()
        .route(
            "/",
            post(|body: Bytes| async move {
                assert_eq!(body.len(), MAX_REQUEST_BODY_BYTES);
                assert!(body.iter().all(|byte| *byte == 0xa5));
                body
            }),
        )
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES));
    let (url, server) = serve(app).await;
    let (tx, mut rx) = mpsc::channel(8);
    let task = tokio::spawn(async move {
        HttpForwarder::new(false)
            .unwrap()
            .execute_stream(
                "sixteen-mib".into(),
                "POST".into(),
                url,
                vec![],
                Some(vec![0xa5; MAX_REQUEST_BODY_BYTES]),
                Some(5_000),
                tx,
                CancellationToken::new(),
            )
            .await;
    });
    let mut received = 0;
    let mut ended = false;
    while let Some(message) = rx.recv().await {
        match message {
            ConnectorToGatewayMessage::HttpResponseStart { status, .. } => assert_eq!(status, 200),
            ConnectorToGatewayMessage::HttpResponseChunk { data, .. } => {
                assert!(data.len() <= RESPONSE_CHUNK_BYTES);
                assert!(data.iter().all(|byte| *byte == 0xa5));
                received += data.len();
            }
            ConnectorToGatewayMessage::HttpResponseEnd { .. } => {
                ended = true;
                break;
            }
            other => panic!("Unexpected response: {other:?}"),
        }
    }
    task.await.unwrap();
    assert!(ended);
    assert_eq!(received, MAX_REQUEST_BODY_BYTES);
    server.abort();
}

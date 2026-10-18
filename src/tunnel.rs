use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use tokio::task::JoinSet;
use tokio_tungstenite::connect_async_with_config;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use url::Url;

use crate::forwarder::{HttpForwarder, MAX_REQUEST_BODY_BYTES};
use crate::outbound::{MAX_WIRE_MESSAGE_BYTES, OutboundFrame, OutboundSender, WirePayload};
use crate::protocol::{ConnectorToGatewayMessage, GatewayToConnectorMessage};

use base64::Engine;
use base64::engine::general_purpose::{STANDARD as BASE64, URL_SAFE, URL_SAFE_NO_PAD};

fn parse_gateway_binary_frame(bin: &[u8]) -> Result<GatewayToConnectorMessage, serde_json::Error> {
    if let Some(nl_idx) = bin.iter().position(|&b| b == b'\n') {
        let header_bytes = &bin[..nl_idx];
        let payload_bytes = &bin[nl_idx + 1..];
        let mut msg: GatewayToConnectorMessage = serde_json::from_slice(header_bytes)?;
        if let GatewayToConnectorMessage::HttpRequest { body, .. } = &mut msg {
            *body = payload_bytes.to_vec();
        }
        Ok(msg)
    } else {
        serde_json::from_slice(bin)
    }
}

#[derive(Clone, Debug)]
pub struct TunnelConfig {
    pub gateway_ws_url: Option<String>,
    pub token: String,
    pub instance_name: Option<String>,
    pub insecure_skip_tls_verify: bool,
    pub ping_interval: Duration,
}

/// Extract the embedded gateway base URL from a `qrc_<base64url>.<secret>` token.
pub fn extract_gateway_url_from_token(token: &str) -> anyhow::Result<String> {
    let trimmed = token.trim();
    let without_prefix = trimmed
        .strip_prefix("qrc_")
        .ok_or_else(|| anyhow::anyhow!("Token must start with 'qrc_'"))?;
    let (url_b64, _secret) = without_prefix.split_once('.').ok_or_else(|| {
        anyhow::anyhow!(
            "Token does not contain an encoded gateway URL (expected 'qrc_<encoded_url>.<secret>' or pass --gateway-url)"
        )
    })?;
    if url_b64.is_empty() {
        anyhow::bail!("Token has an empty encoded gateway URL");
    }
    let decoded_bytes = URL_SAFE_NO_PAD
        .decode(url_b64)
        .or_else(|_| URL_SAFE.decode(url_b64))
        .map_err(|e| anyhow::anyhow!("Failed to decode gateway URL from token: {e}"))?;
    let decoded_url = String::from_utf8(decoded_bytes)
        .map_err(|e| anyhow::anyhow!("Decoded gateway URL in token is not valid UTF-8: {e}"))?;
    Ok(decoded_url)
}

impl TunnelConfig {
    /// Resolve the gateway URL from `--gateway-url` if provided, or decode it from `--token`,
    /// and normalize it into the exact WebSocket endpoint (`wss://.../api/remote-connectors/ws`).
    pub fn normalized_ws_url(&self) -> anyhow::Result<String> {
        let raw_url = match self
            .gateway_ws_url
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            Some(explicit) => explicit.to_string(),
            None => extract_gateway_url_from_token(&self.token)?,
        };

        let mut parsed =
            Url::parse(raw_url.trim()).map_err(|e| anyhow::anyhow!("Invalid gateway URL: {e}"))?;
        if parsed.host_str().is_none() {
            anyhow::bail!("Gateway URL must include a host");
        }
        if !parsed.username().is_empty() || parsed.password().is_some() {
            anyhow::bail!("Gateway URL must not contain credentials");
        }
        parsed.set_query(None);
        parsed.set_fragment(None);

        match parsed.scheme() {
            "https" => {
                let _ = parsed.set_scheme("wss");
            }
            "http" => {
                let _ = parsed.set_scheme("ws");
            }
            "ws" | "wss" => {}
            other => anyhow::bail!("Unsupported gateway URL scheme '{other}'"),
        }

        let path = parsed.path().trim_end_matches('/');
        let ws_path = if path.ends_with("/api/remote-connectors/ws") {
            path.to_string()
        } else {
            format!("{path}/api/remote-connectors/ws")
        };
        parsed.set_path(&ws_path);

        Ok(parsed.to_string())
    }
}

pub const MAX_ACTIVE_STREAMS: usize = 128;
const OUTBOUND_QUEUE_CAPACITY: usize = 512;
const WEBSOCKET_IO_TIMEOUT: Duration = Duration::from_secs(30);
const HEARTBEAT_IDLE_TIMEOUT: Duration = Duration::from_secs(90);

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub async fn run_connector_forever(
    config: TunnelConfig,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let ws_url = config.normalized_ws_url()?;
    let forwarder = Arc::new(HttpForwarder::new(config.insecure_skip_tls_verify)?);
    let mut backoff_ms: u64 = 1_000;
    // Bound the sleep between attempts to 10 seconds plus less than 500 ms jitter.
    let max_backoff_ms: u64 = 10_000;

    info!(gateway_ws_url = %ws_url, "Starting Quilr Remote Connector");

    loop {
        if shutdown.is_cancelled() {
            info!("Shutdown signal received, exiting connector loop");
            return Ok(());
        }

        let mut connected_at = None;
        match run_single_session_tracked(
            &ws_url,
            &config,
            forwarder.clone(),
            shutdown.clone(),
            &mut connected_at,
        )
        .await
        {
            Ok(()) => {
                if shutdown.is_cancelled() {
                    return Ok(());
                }
                info!("WebSocket session closed cleanly; reconnecting...");
            }
            Err(err) => {
                if shutdown.is_cancelled() {
                    return Ok(());
                }
                warn!(error = %err, "WebSocket session terminated; will retry");
            }
        }

        // If the session stayed up for more than 30 seconds, reset the backoff timer.
        if connected_at
            .is_some_and(|connected_at| connected_at.elapsed() >= Duration::from_secs(30))
        {
            backoff_ms = 1_000;
        }

        // Deterministic jitter without external rand crate using current subsec nanos
        let jitter_ms = (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos() as u64)
            % 500;
        let sleep_dur = Duration::from_millis(backoff_ms + jitter_ms);
        info!(
            retry_in_ms = sleep_dur.as_millis(),
            "Waiting before reconnecting to gateway"
        );

        tokio::select! {
            _ = shutdown.cancelled() => {
                return Ok(());
            }
            _ = tokio::time::sleep(sleep_dur) => {}
        }

        backoff_ms = (backoff_ms * 2).min(max_backoff_ms);
    }
}

pub async fn run_single_session(
    ws_url: &str,
    config: &TunnelConfig,
    forwarder: Arc<HttpForwarder>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    run_single_session_tracked(ws_url, config, forwarder, shutdown, &mut None).await
}

async fn run_single_session_tracked(
    ws_url: &str,
    config: &TunnelConfig,
    forwarder: Arc<HttpForwarder>,
    shutdown: CancellationToken,
    connected_at: &mut Option<std::time::Instant>,
) -> anyhow::Result<()> {
    let mut request = ws_url.into_client_request()?;
    let headers = request.headers_mut();
    headers.insert(
        "Authorization",
        http::HeaderValue::from_str(&format!("Bearer {}", config.token.trim()))?,
    );
    headers.insert(
        "X-Quilr-Connector-Token",
        http::HeaderValue::from_str(config.token.trim())?,
    );
    headers.insert(
        "User-Agent",
        http::HeaderValue::from_static(concat!("quilr-connector/", env!("CARGO_PKG_VERSION"))),
    );

    let websocket_config = WebSocketConfig {
        max_write_buffer_size: MAX_WIRE_MESSAGE_BYTES + 128 * 1024,
        max_message_size: Some(MAX_WIRE_MESSAGE_BYTES),
        max_frame_size: Some(MAX_WIRE_MESSAGE_BYTES),
        ..WebSocketConfig::default()
    };
    let (ws_stream, _response) = tokio::select! {
        biased;
        _ = shutdown.cancelled() => return Ok(()),
        result = tokio::time::timeout(WEBSOCKET_IO_TIMEOUT, connect_async_with_config(request, Some(websocket_config), false)) => {
            result.map_err(|_| anyhow::anyhow!("Gateway WebSocket connection timed out"))??
        }
    };
    *connected_at = Some(std::time::Instant::now());
    info!(gateway = %ws_url, "Connected to Quilr MCP Gateway");

    let (mut ws_write, mut ws_read) = ws_stream.split();
    let (outbound_tx, mut outbound_rx) = OutboundSender::channel(OUTBOUND_QUEUE_CAPACITY);

    // Send initial Hello frame
    let host = hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_else(|| "unknown-host".to_string());
    let hello = ConnectorToGatewayMessage::Hello {
        version: env!("CARGO_PKG_VERSION").to_string(),
        hostname: host,
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        pid: std::process::id(),
        instance_name: config.instance_name.clone(),
    };
    outbound_tx
        .send(hello)
        .await
        .map_err(|_| anyhow::anyhow!("Failed to enqueue hello message"))?;

    let mut active_streams: HashMap<String, CancellationToken> = HashMap::new();
    let mut stream_tasks = JoinSet::new();
    let session_cancel = shutdown.child_token();
    // Dropping/aborting this session future must also stop its detached WebSocket writer.
    let _session_cancel_guard = session_cancel.clone().drop_guard();

    // A write failure must also interrupt a blocked reader and every upstream task.
    let writer_cancel = session_cancel.clone();
    let ping_interval = config.ping_interval.max(Duration::from_secs(1));
    let writer_handle = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(ping_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await;
        let result: anyhow::Result<()> = async {
            loop {
                let msg = tokio::select! {
                    biased;
                    _ = writer_cancel.cancelled() => break,
                    _ = ticker.tick() => OutboundFrame::control(ConnectorToGatewayMessage::Ping { ts: unix_ms() })?,
                    maybe_msg = outbound_rx.recv() => match maybe_msg {
                        Some(msg) => msg,
                        None => break,
                    },
                };
                let OutboundFrame { payload, _permit } = msg;
                let ws_msg = match payload {
                    WirePayload::Text(json) => Message::Text(json),
                    WirePayload::Binary(bytes) => Message::Binary(bytes),
                };
                tokio::select! {
                    biased;
                    _ = writer_cancel.cancelled() => break,
                    result = tokio::time::timeout(WEBSOCKET_IO_TIMEOUT, ws_write.send(ws_msg)) => {
                        result.map_err(|_| anyhow::anyhow!("WebSocket write timed out"))??;
                    }
                }
            }
            Ok(())
        }.await;
        writer_cancel.cancel();
        // The peer may stop reading during shutdown. Never await its close handshake forever.
        let _ = tokio::time::timeout(Duration::from_secs(1), ws_write.close()).await;
        result
    });

    // Reader loop
    let mut session_result: anyhow::Result<()> = Ok(());
    let mut last_frame_at = tokio::time::Instant::now();
    loop {
        tokio::select! {
            _ = session_cancel.cancelled() => {
                break;
            }
            finished = stream_tasks.join_next(), if !stream_tasks.is_empty() => {
                match finished {
                    Some(Ok(stream_id)) => { active_streams.remove(&stream_id); }
                    Some(Err(err)) => {
                        session_result = Err(anyhow::anyhow!("HTTP forwarding task failed: {err}"));
                        break;
                    }
                    None => {}
                }
            }
            result = tokio::time::timeout_at(last_frame_at + HEARTBEAT_IDLE_TIMEOUT, ws_read.next()) => {
                let next_frame = match result {
                    Ok(frame) => frame,
                    Err(_) => {
                        session_result = Err(anyhow::anyhow!("Gateway WebSocket heartbeat timed out"));
                        break;
                    }
                };
                last_frame_at = tokio::time::Instant::now();
                let parsed_message = match next_frame {
                    Some(Ok(Message::Text(text))) => Some(serde_json::from_str::<GatewayToConnectorMessage>(&text)),
                    Some(Ok(Message::Binary(bin))) => Some(parse_gateway_binary_frame(&bin)),
                    Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) | Some(Ok(Message::Frame(_))) => None,
                    Some(Ok(Message::Close(frame))) => {
                        if let Some(cf) = frame {
                            let code: u16 = cf.code.into();
                            if code == 4401 || code == 4403 {
                                session_result = Err(anyhow::anyhow!(
                                    "Gateway rejected connector authentication (code {code}: {})",
                                    cf.reason
                                ));
                            } else {
                                info!(code = code, reason = %cf.reason, "Gateway closed WebSocket");
                            }
                        }
                        break;
                    }
                    Some(Err(err)) => {
                        session_result = Err(anyhow::anyhow!("WebSocket read error: {err}"));
                        break;
                    }
                    None => {
                        break;
                    }
                };

                if let Some(parsed) = parsed_message {
                    match parsed {
                        Ok(GatewayToConnectorMessage::HttpRequest {
                            stream_id,
                            method,
                            url,
                            headers,
                            body,
                            body_b64,
                            timeout_ms,
                        }) => {
                            debug!(stream_id = %stream_id, method = %method, url = %url, "Dispatching HTTP request");
                            if stream_id.is_empty() || active_streams.contains_key(&stream_id) {
                                session_result = Err(anyhow::anyhow!("Invalid or duplicate active stream ID"));
                                break;
                            }
                            if active_streams.len() >= MAX_ACTIVE_STREAMS {
                                if outbound_tx.try_send(ConnectorToGatewayMessage::HttpResponseError {
                                    stream_id,
                                    error: "Remote connector maximum active stream capacity reached".to_string(),
                                }).is_err() {
                                    session_result = Err(anyhow::anyhow!("WebSocket outbound queue full"));
                                    break;
                                }
                                continue;
                            }
                            let resolved_body = if !body.is_empty() {
                                Some(body)
                            } else if let Some(b64) = body_b64.filter(|s| !s.is_empty()) {
                                if b64.len() > MAX_REQUEST_BODY_BYTES.div_ceil(3) * 4 {
                                    if outbound_tx.try_send(ConnectorToGatewayMessage::HttpResponseError {
                                        stream_id,
                                        error: "Request body exceeds maximum size".to_string(),
                                    }).is_err() {
                                        session_result = Err(anyhow::anyhow!("WebSocket outbound queue full"));
                                        break;
                                    }
                                    continue;
                                }
                                match BASE64.decode(b64) {
                                    Ok(decoded) => Some(decoded),
                                    Err(err) => {
                                        if outbound_tx.try_send(ConnectorToGatewayMessage::HttpResponseError {
                                            stream_id,
                                            error: format!("Invalid base64 request body: {err}"),
                                        }).is_err() {
                                            session_result = Err(anyhow::anyhow!("WebSocket outbound queue full"));
                                            break;
                                        }
                                        continue;
                                    }
                                }
                            } else {
                                None
                            };
                            let stream_token = session_cancel.child_token();
                            active_streams.insert(stream_id.clone(), stream_token.clone());
                            let fwd = forwarder.clone();
                            let tx = outbound_tx.clone();
                            stream_tasks.spawn(async move {
                                fwd.execute_stream(
                                    stream_id.clone(), method, url, headers, resolved_body,
                                    timeout_ms, tx, stream_token,
                                ).await;
                                stream_id
                            });
                        }
                        Ok(GatewayToConnectorMessage::CancelStream { stream_id }) => {
                            // Keep the ID reserved until its task exits, including blocked queue sends.
                            if let Some(token) = active_streams.get(&stream_id) {
                                token.cancel();
                            }
                        }
                        Ok(GatewayToConnectorMessage::Ping { ts }) => {
                            if outbound_tx.try_send(ConnectorToGatewayMessage::Pong { ts }).is_err() {
                                session_result = Err(anyhow::anyhow!("WebSocket outbound queue full"));
                                break;
                            }
                        }
                        Ok(GatewayToConnectorMessage::Pong { .. }) => {}
                        Err(err) => {
                            warn!(error = %err, "Ignored unrecognized message from gateway");
                        }
                    }
                }
            }
        }
    }

    // Join cancelled tasks so a reconnect cannot leave upstream requests running.
    session_cancel.cancel();
    for (_, token) in active_streams.drain() {
        token.cancel();
    }
    while stream_tasks.join_next().await.is_some() {}
    match writer_handle.await {
        Ok(Err(err)) if session_result.is_ok() => session_result = Err(err),
        Err(err) if session_result.is_ok() => session_result = Err(err.into()),
        _ => {}
    }

    session_result
}

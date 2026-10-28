use std::collections::HashSet;
use std::str::FromStr;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use reqwest::Method;
use tokio_util::sync::CancellationToken;
use tracing::{Level, info, warn};
use url::Url;

use crate::outbound::OutboundSender;
use crate::protocol::ConnectorToGatewayMessage;
use crate::telemetry;

/// Bound both WebSocket frame size and memory retained by the outbound queue.
pub const RESPONSE_CHUNK_BYTES: usize = 64 * 1024;
pub const MAX_REQUEST_BODY_BYTES: usize = 16 * 1024 * 1024;

/// Hop-by-hop HTTP headers that must not be forwarded between proxy hops.
const HOP_BY_HOP_HEADERS: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
];

fn connection_headers(headers: &[(String, String)]) -> HashSet<String> {
    headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("connection"))
        .flat_map(|(_, value)| value.split(','))
        .map(|name| name.trim().to_ascii_lowercase())
        .collect()
}

fn is_hop_by_hop(name: &str, connection_headers: &HashSet<String>) -> bool {
    let lower = name.trim().to_ascii_lowercase();
    HOP_BY_HOP_HEADERS.contains(&lower.as_str()) || connection_headers.contains(&lower)
}

/// Coarse failure category for logs; the full message is only sent to the gateway.
#[derive(Debug, Clone, Copy)]
enum ErrorKind {
    InvalidRequest,
    Timeout,
    Connect,
    Upstream,
    Gateway,
}

impl ErrorKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid_request",
            Self::Timeout => "timeout",
            Self::Connect => "connect",
            Self::Upstream => "upstream",
            Self::Gateway => "gateway_queue",
        }
    }

    fn of(err: &anyhow::Error) -> Self {
        err.downcast_ref::<KindedError>()
            .map_or(Self::InvalidRequest, |e| e.kind)
    }
}

/// An error message tagged with its [`ErrorKind`]; displays as the bare message.
#[derive(Debug)]
struct KindedError {
    kind: ErrorKind,
    message: String,
}

impl std::fmt::Display for KindedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for KindedError {}

fn kinded(kind: ErrorKind, message: impl Into<String>) -> anyhow::Error {
    KindedError {
        kind,
        message: message.into(),
    }
    .into()
}

/// What a stream produced, filled in as it progresses.
#[derive(Default)]
struct Progress {
    status: Option<u16>,
    bytes: u64,
    sse: bool,
}

/// Every queue wait must release the upstream response when the stream/session is cancelled.
async fn enqueue(
    outbound_tx: &OutboundSender,
    cancel_token: &CancellationToken,
    message: ConnectorToGatewayMessage,
) -> anyhow::Result<bool> {
    tokio::select! {
        biased;
        _ = cancel_token.cancelled() => Ok(false),
        result = outbound_tx.send(message) => {
            result.map_err(|_| kinded(ErrorKind::Gateway, "WebSocket outbound channel closed"))?;
            Ok(true)
        }
    }
}

async fn idle_timeout<T>(
    timeout: Option<Duration>,
    future: impl std::future::Future<Output = T>,
) -> anyhow::Result<T> {
    match timeout {
        Some(timeout) => tokio::time::timeout(timeout, future)
            .await
            .map_err(|_| kinded(ErrorKind::Timeout, "Upstream request idle timeout")),
        None => Ok(future.await),
    }
}

pub struct HttpForwarder {
    client: reqwest::Client,
}

impl HttpForwarder {
    pub fn new(insecure_skip_tls_verify: bool) -> anyhow::Result<Self> {
        // Keep encoded bytes and Content-Encoding together. The receiving HTTP client
        // performs decoding, including encodings not understood by this binary.
        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(insecure_skip_tls_verify)
            .redirect(reqwest::redirect::Policy::none())
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .pool_idle_timeout(Duration::from_secs(60))
            .pool_max_idle_per_host(2)
            .connect_timeout(Duration::from_secs(30))
            .build()?;
        Ok(Self { client })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn execute_stream(
        &self,
        stream_id: String,
        method: String,
        target_url: String,
        headers: Vec<(String, String)>,
        body: Option<Vec<u8>>,
        timeout_ms: Option<u64>,
        outbound_tx: impl Into<OutboundSender>,
        cancel_token: CancellationToken,
    ) {
        let outbound_tx = outbound_tx.into();
        let _active = telemetry::ActiveStream::begin();
        let started = Instant::now();
        let mut progress = Progress::default();
        let result = self
            .run_inner(
                &stream_id,
                &method,
                &target_url,
                headers,
                body,
                timeout_ms,
                &outbound_tx,
                &cancel_token,
                &mut progress,
            )
            .await;

        // Logs carry only the target host; the path is added at debug level.
        let parsed_url = Url::parse(&target_url).ok();
        let target = parsed_url
            .as_ref()
            .map(|url| match (url.host_str(), url.port()) {
                (Some(host), Some(port)) => format!("{host}:{port}"),
                (Some(host), None) => host.to_string(),
                _ => String::new(),
            });
        let path = parsed_url
            .as_ref()
            .filter(|_| tracing::enabled!(Level::DEBUG))
            .map(|url| url.path().to_string());
        let duration_ms = started.elapsed().as_millis() as u64;
        let failure = result
            .as_ref()
            .err()
            .filter(|_| !cancel_token.is_cancelled());
        telemetry::record_request(failure.is_some());
        let outcome = if failure.is_some() {
            "error"
        } else if cancel_token.is_cancelled() {
            "cancelled"
        } else {
            "ok"
        };
        macro_rules! log_request {
            ($level:ident, $($extra:tt)*) => {
                $level!(
                    event = "request",
                    method = %method.to_ascii_uppercase(),
                    target = target.as_deref(),
                    path = path.as_deref(),
                    status = progress.status,
                    duration_ms,
                    bytes = progress.bytes,
                    sse = progress.sse,
                    outcome,
                    $($extra)*
                )
            };
        }
        match failure {
            Some(err) => log_request!(
                warn,
                error_kind = ErrorKind::of(err).as_str(),
                "Request failed"
            ),
            None if outcome == "cancelled" => log_request!(info, "Request cancelled"),
            None => log_request!(info, "Request completed"),
        }

        if let Err(err) = result
            && !cancel_token.is_cancelled()
        {
            let _ = enqueue(
                &outbound_tx,
                &cancel_token,
                ConnectorToGatewayMessage::HttpResponseError {
                    stream_id,
                    error: err.to_string(),
                },
            )
            .await;
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_inner(
        &self,
        stream_id: &str,
        method_str: &str,
        target_url: &str,
        headers: Vec<(String, String)>,
        body: Option<Vec<u8>>,
        timeout_ms: Option<u64>,
        outbound_tx: &OutboundSender,
        cancel_token: &CancellationToken,
        progress: &mut Progress,
    ) -> anyhow::Result<()> {
        let parsed_url = Url::parse(target_url)
            .map_err(|e| anyhow::anyhow!("Invalid target URL '{target_url}': {e}"))?;
        match parsed_url.scheme() {
            "http" | "https" => {}
            other => {
                anyhow::bail!("Unsupported URL scheme '{other}' (only http and https are allowed)")
            }
        }

        let method = Method::from_str(&method_str.to_ascii_uppercase())
            .map_err(|e| anyhow::anyhow!("Invalid HTTP method '{method_str}': {e}"))?;

        let mut req_builder = self.client.request(method, parsed_url);
        let request_connection_headers = connection_headers(&headers);
        for (k, v) in headers {
            if is_hop_by_hop(&k, &request_connection_headers) {
                continue;
            }
            let value: Vec<u8> = v
                .chars()
                .map(|ch| {
                    u8::try_from(u32::from(ch))
                        .map_err(|_| anyhow::anyhow!("Header value is not Latin-1"))
                })
                .collect::<anyhow::Result<_>>()?;
            req_builder = req_builder.header(k, reqwest::header::HeaderValue::from_bytes(&value)?);
        }

        if let Some(raw_body) = body.filter(|value| !value.is_empty()) {
            if raw_body.len() > MAX_REQUEST_BODY_BYTES {
                anyhow::bail!("Request body exceeds maximum size");
            }
            req_builder = req_builder.body(raw_body);
        }

        // The wire timeout is an idle read timeout, not a total SSE lifetime.
        // None/zero means unlimited, matching httpx read=None on the gateway.
        let timeout = timeout_ms.filter(|ms| *ms > 0).map(Duration::from_millis);
        let response = tokio::select! {
            biased;
            _ = cancel_token.cancelled() => return Ok(()),
            res = idle_timeout(timeout, req_builder.send()) => {
                res?.map_err(|e| {
                    let kind = if e.is_timeout() {
                        ErrorKind::Timeout
                    } else if e.is_connect() {
                        ErrorKind::Connect
                    } else {
                        ErrorKind::Upstream
                    };
                    kinded(kind, format!("Upstream request error: {e}"))
                })?
            }
        };

        let status = response.status().as_u16();
        progress.status = Some(status);
        progress.sse = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("text/event-stream"));
        let raw_headers: Vec<_> = response
            .headers()
            .iter()
            .map(|(name, value)| {
                // HTTP header values can contain obs-text bytes. Preserve them across
                // JSON using the same Latin-1 mapping used by the gateway transports.
                (
                    name.to_string(),
                    value.as_bytes().iter().map(|b| char::from(*b)).collect(),
                )
            })
            .collect();
        let response_connection_headers = connection_headers(&raw_headers);
        let resp_headers = raw_headers
            .into_iter()
            .filter(|(name, _)| !is_hop_by_hop(name, &response_connection_headers))
            .collect();

        if !enqueue(
            outbound_tx,
            cancel_token,
            ConnectorToGatewayMessage::HttpResponseStart {
                stream_id: stream_id.to_string(),
                status,
                headers: resp_headers,
            },
        )
        .await?
        {
            return Ok(());
        }

        let mut byte_stream = response.bytes_stream();
        loop {
            let next_chunk = tokio::select! {
                biased;
                _ = cancel_token.cancelled() => return Ok(()),
                result = idle_timeout(timeout, byte_stream.next()) => result?,
            };
            match next_chunk {
                Some(Ok(bytes)) => {
                    progress.bytes += bytes.len() as u64;
                    for chunk in bytes.chunks(RESPONSE_CHUNK_BYTES) {
                        if !enqueue(
                            outbound_tx,
                            cancel_token,
                            ConnectorToGatewayMessage::HttpResponseChunk {
                                stream_id: stream_id.to_string(),
                                data: chunk.to_vec(),
                            },
                        )
                        .await?
                        {
                            return Ok(());
                        }
                    }
                }
                Some(Err(err)) => {
                    return Err(kinded(
                        ErrorKind::Upstream,
                        format!("Stream read error: {err}"),
                    ));
                }
                None => {
                    enqueue(
                        outbound_tx,
                        cancel_token,
                        ConnectorToGatewayMessage::HttpResponseEnd {
                            stream_id: stream_id.to_string(),
                        },
                    )
                    .await?;
                    return Ok(());
                }
            }
        }
    }
}

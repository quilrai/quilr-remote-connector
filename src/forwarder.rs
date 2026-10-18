use std::collections::HashSet;
use std::str::FromStr;
use std::time::Duration;

use futures_util::StreamExt;
use reqwest::Method;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use url::Url;

use crate::outbound::OutboundSender;
use crate::protocol::ConnectorToGatewayMessage;

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
            result.map_err(|_| anyhow::anyhow!("WebSocket outbound channel closed"))?;
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
            .map_err(|_| anyhow::anyhow!("Upstream request idle timeout")),
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
            )
            .await;

        if let Err(err) = result
            && !cancel_token.is_cancelled()
        {
            debug!(stream_id = %stream_id, error = %err, "Stream failed with error");
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
                res?.map_err(|e| anyhow::anyhow!("Upstream request error: {e}"))?
            }
        };

        let status = response.status().as_u16();
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
                _ = cancel_token.cancelled() => {
                    debug!(stream_id = %stream_id, "Stream cancelled by gateway");
                    return Ok(());
                }
                result = idle_timeout(timeout, byte_stream.next()) => result?,
            };
            match next_chunk {
                Some(Ok(bytes)) => {
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
                    warn!(stream_id = %stream_id, error = %err, "Error reading upstream body stream");
                    return Err(anyhow::anyhow!("Stream read error: {err}"));
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

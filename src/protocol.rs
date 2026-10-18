use serde::{Deserialize, Serialize};

/// Messages sent from the Quilr MCP Gateway to the Remote Connector over WebSocket.
///
/// Control messages and bodyless requests are sent as WebSocket Text frames (JSON).
/// `HttpRequest` messages with a non-empty body are sent as WebSocket Binary frames
/// formatted as `<single-line JSON header>\n<raw body bytes>`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GatewayToConnectorMessage {
    /// Start an outbound HTTP/SSE request to a local/cluster MCP target.
    HttpRequest {
        stream_id: String,
        method: String,
        url: String,
        #[serde(default)]
        headers: Vec<(String, String)>,
        /// Raw request body bytes populated when received as a binary frame.
        #[serde(skip)]
        body: Vec<u8>,
        /// Optional legacy base64 body fallback if sent in a JSON text frame.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        body_b64: Option<String>,
        /// Idle timeout for response headers and each body read; zero/None is unlimited.
        #[serde(default)]
        timeout_ms: Option<u64>,
    },
    /// Cancel an in-flight HTTP/SSE request stream.
    CancelStream { stream_id: String },
    /// Application-level heartbeat ping from the gateway.
    Ping {
        #[serde(default)]
        ts: Option<u64>,
    },
    /// Application-level heartbeat pong from the gateway.
    Pong {
        #[serde(default)]
        ts: Option<u64>,
    },
}

/// Messages sent from the Remote Connector to the Quilr MCP Gateway over WebSocket.
///
/// Control messages are sent as WebSocket Text frames (JSON).
/// `HttpResponseChunk` is sent as a WebSocket Binary frame formatted as
/// `{"type":"http_response_chunk","stream_id":"..."}\n<raw chunk bytes>`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ConnectorToGatewayMessage {
    /// Initial handshake frame sent immediately after WebSocket connect.
    Hello {
        version: String,
        hostname: String,
        os: String,
        arch: String,
        pid: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        instance_name: Option<String>,
    },
    /// HTTP response status code and headers for a stream.
    HttpResponseStart {
        stream_id: String,
        status: u16,
        headers: Vec<(String, String)>,
    },
    /// A chunk of the HTTP/SSE response body carried as raw bytes in a binary WebSocket frame.
    HttpResponseChunk {
        stream_id: String,
        #[serde(skip)]
        data: Vec<u8>,
    },
    /// Clean EOF of the HTTP/SSE response body.
    HttpResponseEnd { stream_id: String },
    /// Transport or connection error while executing the HTTP request or reading its stream.
    HttpResponseError { stream_id: String, error: String },
    /// Application-level heartbeat ping from the connector.
    Ping { ts: u64 },
    /// Application-level heartbeat pong replying to a gateway ping.
    Pong {
        #[serde(skip_serializing_if = "Option::is_none")]
        ts: Option<u64>,
    },
}

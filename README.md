# Quilr Remote Connector (`quilr-connector`)

Lightweight Rust WebSocket reverse-tunnel binary that connects private MCP servers and internal REST APIs (URL-to-MCP) running inside a Kubernetes cluster, VM, or developer laptop to the **Quilr MCP Gateway**.

## Features

- **Self-Contained Token**: Tokens (`qrc_<encoded_gateway_url>.<secret>`) generated in the Quilr Admin Console encode the MCP Gateway URL directly — no separate URL configuration is required.
- **Outbound-Only WebSocket Tunnel**: No inbound firewall ports or public ingress required. Connects out to `wss://<gateway>/api/remote-connectors/ws`.
- **Zero-Bloat Binary Framing**: Transfers request bodies and response chunks using WebSocket Binary frames (`<single-line JSON header>\n<raw bytes>`) with zero Base64 size expansion.
- **Multiplexed HTTP, REST API & SSE Streaming**: Handles concurrent MCP Streamable HTTP (`POST`/`GET`/`DELETE`), legacy `SSE` streams, and REST API (`GET`/`POST`/`PUT`/`PATCH`/`DELETE`/`HEAD`) + OpenAPI specification requests for URL-to-MCP backends over a single WebSocket connection.
- **High Availability (Multi-Replica)**: Run 2+ connector pods with the same token for load balancing and failover of subsequent requests. In-flight streams fail on disconnect and are never replayed.
- **Automatic Reconnection**: Exponential backoff with jitter and periodic application-level ping/pong heartbeats.

## Usage

### 1. Standalone Binary (Linux, macOS, Windows)

Download the prebuilt binary for your platform from [GitHub Releases](https://github.com/quilrai/local-to-quilr-saas-connector/releases) (`x86_64-unknown-linux-musl`, `aarch64-unknown-linux-musl`, `x86_64-apple-darwin`, `aarch64-apple-darwin`, `x86_64-pc-windows-msvc`):

```bash
quilr-connector --token qrc_your_token_here
```

Or via environment variable:

```bash
export QUILR_CONNECTOR_TOKEN="qrc_your_token_here"
quilr-connector
```

*(Optional: `--gateway-url` / `QUILR_GATEWAY_URL` can be passed to override the gateway URL embedded in the token.)*

### 2. Docker

```bash
docker run -d --name quilr-connector --restart unless-stopped \
  -e QUILR_CONNECTOR_TOKEN="qrc_your_token_here" \
  ghcr.io/quilrai/quilr-connector:latest
```

### 3. Kubernetes Deployment

Apply `k8s/deployment.yaml` after setting `QUILR_CONNECTOR_TOKEN`:

```bash
kubectl apply -f k8s/deployment.yaml
```

## Tunnel Limits and Testing

Each worker permits 128 concurrent upstream streams and a 16 MiB request body. WebSocket messages and frames permit 32 MiB. Response frames contain at most 64 KiB of body data. The outbound queue holds at most 512 frames and 32 MiB of serialized data, with byte accounting retained until the socket write completes. The wire `timeout_ms` is an idle timeout for response headers and each body read; zero or an omitted timeout permits an unlimited stream lifetime. HTTP content encodings and header bytes pass through to the gateway client for decoding.

WebSocket connection attempts (TCP, TLS, and HTTP Upgrade) and writes time out after 30 seconds. A gateway that sends no frames is disconnected after 90 seconds. Session loss cancels and joins every forwarding task before reconnecting; dropping an upstream response closes its body/socket. Retry backoff starts at one second and caps at 10 seconds, plus less than 500 ms jitter. A healthy session of at least 30 seconds resets backoff. Idle upstream HTTP connections expire after 60 seconds, with at most two retained per host.

Run `cargo test` for local regression tests. An optional cross-language smoke test starts two real connector processes with the Python gateway hub and a loopback HTTP/SSE server:

```bash
cargo build
/path/to/gateway/.venv/bin/python tests/wire_smoke.py \
  --gateway-root /path/to/gateway/mcpgateway \
  --connector-binary target/debug/quilr-connector
```

The smoke test checks encoded and decoded gzip, SSE idle timeouts, cancellation, a 16 MiB request/response roundtrip, stream churn, replica failover, and gateway outage/restart recovery while the connector stays running. It requires the gateway Python dependencies and does not access a database or an external service.

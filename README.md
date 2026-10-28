# Quilr Remote Connector (`quilr-connector`)

Connects private MCP servers, REST APIs, and LLM endpoints (in a VM, Kubernetes cluster, or laptop) to the Quilr gateway over one outbound WebSocket. No inbound ports needed.

- Token from the Quilr Admin Console (`qrc_…`) carries the gateway URL. Nothing else to configure.
- Multiplexes HTTP, REST, and SSE streams. Run 2+ instances with the same token for HA.
- Reconnects automatically. Runs as a boot-time service. Writes JSON logs.

## Install

Download a binary from the [latest release](https://github.com/quilrai/quilr-remote-connector/releases/latest). Builds are available for Linux x86_64/arm64 (static musl), macOS, and Windows.

## Run

```bash
quilr-connector --token qrc_...            # foreground (or set QUILR_CONNECTOR_TOKEN)
```

### As a service (Linux/systemd, starts at boot)

```bash
sudo ./quilr-connector service install --token qrc_...
sudo quilr-connector service status
sudo quilr-connector service logs -f
sudo quilr-connector service start|stop|restart|uninstall
```

- Installs to `/usr/local/bin`. Writes config to `/etc/quilr-connector/connector.env` (root-only) and a sandboxed systemd unit with auto-restart.
- Upgrade: re-run `service install` with the new binary. The existing config is kept unless you pass `--token`.
- Without `sudo` it installs a per-user service instead, with the binary in `~/.local/bin`. To make that start at boot, run `sudo loginctl enable-linger $USER`.
- Upstream HTTPS trusts the system CA store (`/etc/ssl/certs`) as well as the bundled roots.
- macOS: the same commands use launchd. Windows: use a service wrapper such as NSSM.

### Docker / Kubernetes

```bash
docker run -d --restart unless-stopped -e QUILR_CONNECTOR_TOKEN=qrc_... ghcr.io/quilrai/quilr-connector:latest
kubectl apply -f k8s/deployment.yaml      # set QUILR_CONNECTOR_TOKEN first
```

## Options

Every flag also works with `service install`.

| Flag | Env | Default |
| --- | --- | --- |
| `--token` | `QUILR_CONNECTOR_TOKEN` | required |
| `--gateway-url` | `QUILR_GATEWAY_URL` | from token |
| `--instance-name` | `QUILR_CONNECTOR_INSTANCE_NAME` | hostname |
| `--insecure-skip-tls-verify` | `QUILR_CONNECTOR_INSECURE_TLS` | off |
| `--log-level error\|warn\|info\|debug` | `QUILR_LOG_LEVEL` | `info` |
| `--log-format auto\|json\|text` | `QUILR_LOG_FORMAT` | text on a terminal, else JSON |
| `--log-file <path>` | `QUILR_LOG_FILE` | stdout. Rotates daily, keeps 7 files. Service: use `/var/log/quilr-connector/` |
| `--heartbeat-log-secs` | `QUILR_HEARTBEAT_LOG_SECS` | `300` (`0` = off) |

## Logs

One JSON object per line, each with an `event` field. Logs never include tokens, headers, bodies, or query strings. URL paths appear only at `debug` level.

| `event` | Meaning |
| --- | --- |
| `startup` `connected` `shutdown` `stopped` | Lifecycle |
| `connect_failed` `disconnected` `auth_rejected` | Gateway problems (`reason`, `retry_in_ms`) |
| `request` | One per forwarded call: `method`, `target`, `status`, `duration_ms`, `bytes`, `sse`, `outcome`, `error_kind` |
| `request_rejected` | Concurrency limit hit |
| `heartbeat` | Every 5 min: `connected`, `active_streams`, `requests`/`errors`/`disconnects` since the previous heartbeat |

```json
{"timestamp":"2026-10-28T10:01:02.123456Z","level":"INFO","message":"Request completed","event":"request","method":"POST","target":"mcp.internal:8080","status":200,"duration_ms":184,"bytes":912,"sse":false,"outcome":"ok"}
```

## Sizing

**Recommended: 2 vCPU / 3 GB RAM** per instance. Minimum: 1 vCPU / 1 GB if large (multi-MiB) request bodies aren't expected.

Measured on loopback with the release build limited to 2 worker threads:

| Load | Peak memory | CPU |
| --- | --- | --- |
| Idle, connected | 5 MiB | ~0% |
| Small JSON requests, 128 concurrent (~30k req/s, p99 7.5 ms) | 14 MiB | 2 cores |
| 128 open SSE streams | 16 MiB | <1% of a core |
| 1 MiB responses, 128 concurrent | ~320 MiB | 1.5 cores |
| Worst case: 128 × 16 MiB uploads to a slow upstream | ~2.1 GiB | — |

CPU is not the constraint: realistic MCP/LLM traffic uses well under one core. The memory worst case is bounded by the limits below (128 streams × 16 MiB + 32 MiB queue), so 3 GB covers it. At `info` level, a `request` log line is about 220 bytes, which is about 18 MiB per 100k requests.

## Limits

128 concurrent streams per instance, 16 MiB request body, 64 KiB response chunks, 32 MiB outbound queue. Reconnect backoff is 1–10 s. The connector drops the session if the gateway is silent for 90 s. In-flight streams fail on disconnect and are never replayed.

## Development

```bash
cargo fmt --all -- --check && cargo clippy --all-targets -- -D warnings && cargo test --locked
```

Optional cross-language smoke test against the Python gateway: `python tests/wire_smoke.py --gateway-root <mcpgateway> --connector-binary target/debug/quilr-connector`.

**Release:** bump the version in `Cargo.toml` and `Cargo.lock`, push to `main`, then `git tag -a vX.Y.Z -m "Release vX.Y.Z" && git push origin vX.Y.Z`. The [release workflow](https://github.com/quilrai/quilr-remote-connector/actions/workflows/release.yml) builds all binaries, checksums, and the Docker image, and publishes the GitHub Release. To rebuild an existing tag (v0.1.1+): `gh workflow run release.yml --ref main -f tag=vX.Y.Z`.

"""Opt-in integration check using the actual Rust binary and Python tunnel classes.

Run with the gateway virtualenv Python, passing --gateway-root and --connector-binary.
All sockets bind loopback ephemeral ports; no database or external network is used.
"""

import argparse
import asyncio
import contextlib
import gzip
import socket
import sys
from pathlib import Path


async def run(gateway_root: Path, connector_binary: Path) -> None:
    sys.path.insert(0, str(gateway_root))
    import httpx
    import uvicorn
    from fastapi import FastAPI, WebSocket
    from services.remote_connector.hub import (
        ConnectorWorkerSession,
        RemoteConnectorDisconnectedError,
        RemoteConnectorHub,
    )
    from services.remote_connector.transport import HttpxTunnelTransport

    expected = b'{"jsonrpc":"2.0","result":"compressed response"}'
    encoded = gzip.compress(expected)
    body_limit = 16 * 1024 * 1024
    cancelled_upstream = asyncio.Event()
    upstream_tasks: set[asyncio.Task] = set()

    async def upstream(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        upstream_tasks.add(task)
        try:
            request = await reader.readuntil(b"\r\n\r\n")
            path = request.split(b" ", 2)[1]
            if path == b"/echo":
                headers = dict(line.split(b":", 1) for line in request.split(b"\r\n")[1:] if b":" in line)
                content_length = int(headers.get(b"content-length", b"0"))
                assert content_length == body_limit
                body = await reader.readexactly(content_length)
                assert body == b"\xa5" * body_limit
                writer.write(
                    b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: "
                    + str(len(body)).encode() + b"\r\n\r\n" + body
                )
                await writer.drain()
            elif path == b"/gzip":
                writer.write(
                    b"HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Type: application/json\r\n"
                    b"Connection: close\r\nContent-Length: "
                    + str(len(encoded)).encode()
                    + b"\r\n\r\n"
                    + encoded
                )
                await writer.drain()
            else:
                writer.write(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
                    b"Transfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
                )
                if path == b"/sse":
                    for n in range(4):
                        await asyncio.sleep(0.08)
                        chunk = f"data: {n}\n\n".encode()
                        writer.write(f"{len(chunk):x}\r\n".encode() + chunk + b"\r\n")
                        await writer.drain()
                    writer.write(b"0\r\n\r\n")
                    await writer.drain()
                elif path == b"/hang":
                    writer.write(b"7\r\ndata: 0\r\n")
                    await writer.drain()
                    await reader.read()
                    cancelled_upstream.set()
                else:
                    raise AssertionError(f"Unexpected path {path!r}")
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()
            upstream_tasks.discard(task)

    hub = RemoteConnectorHub()
    app = FastAPI()
    session_number = 0

    @app.websocket("/api/remote-connectors/ws")
    async def tunnel(websocket: WebSocket) -> None:
        nonlocal session_number
        await websocket.accept()
        session_number += 1
        session = ConnectorWorkerSession(
            session_id=f"session-{session_number}", connector_id="connector", tenant_id="tenant",
            websocket=websocket,
        )
        session_hub = hub
        session_hub.register_session(session)
        try:
            await session.run()
        finally:
            session_hub.unregister_session(session)

    async def wait_for(predicate, timeout=5) -> None:
        async with asyncio.timeout(timeout):
            while not predicate():
                await asyncio.sleep(0.01)

    async def collect(stream, session=None) -> bytes:
        chunks = []
        try:
            async with asyncio.timeout(5):
                while True:
                    item = await stream.body_queue.get()
                    if item is None:
                        return b"".join(chunks)
                    if isinstance(item, Exception):
                        raise item
                    stream.on_chunk_consumed(len(item))
                    chunks.append(item)
        finally:
            if session is not None:
                session.cancel_stream_local(stream.stream_id, send_remote_cancel=False)

    upstream_server = await asyncio.start_server(upstream, "127.0.0.1", 0)
    upstream_url = f"http://127.0.0.1:{upstream_server.sockets[0].getsockname()[1]}"
    async def start_gateway(port=0):
        listener = socket.socket()
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", port))
        bound_port = listener.getsockname()[1]
        server = uvicorn.Server(uvicorn.Config(
            app, log_level="error", lifespan="off", ws_max_size=32 * 1024 * 1024,
            timeout_graceful_shutdown=1,
        ))
        server_task = asyncio.create_task(server.serve(sockets=[listener]))
        await wait_for(lambda: server.started)
        return server, server_task, bound_port

    server, server_task, gateway_port = await start_gateway()
    gateway_url = f"http://127.0.0.1:{gateway_port}"
    processes = []
    try:
        await wait_for(lambda: server.started)
        for _ in range(2):
            processes.append(await asyncio.create_subprocess_exec(
                str(connector_binary), "--gateway-url", gateway_url, "--token", "qrc_test",
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
            ))
        await wait_for(lambda: hub.active_session_count("connector") == 2)
        await wait_for(lambda: all(instance["pid"] for instance in hub.get_instances("connector")))

        # Inspect the wire bytes directly before httpx decoding.
        stream, session = await hub.open_stream(
            connector_id="connector", method="GET", url=upstream_url + "/gzip", headers=[], body=b"",
            timeout_ms=500,
        )
        status, headers = await asyncio.wait_for(stream.headers_future, 5)
        assert status == 200 and ("content-encoding", "gzip") in headers
        assert await collect(stream, session) == encoded

        async with httpx.AsyncClient(transport=HttpxTunnelTransport("connector", hub), timeout=0.2) as client:
            response = await client.get(upstream_url + "/gzip")
            assert response.content == expected, "gzip must decode exactly once"
            response = await client.get(upstream_url + "/sse")
            assert response.content == b"".join(f"data: {n}\n\n".encode() for n in range(4))
            response = await client.post(upstream_url + "/echo", content=b"\xa5" * body_limit, timeout=5)
            assert response.content == b"\xa5" * body_limit, "16 MiB bodies must survive both tunnel directions"

            for _ in range(256):
                response = await client.get(upstream_url + "/gzip")
                assert response.content == expected
            await wait_for(lambda: all(not session.streams for session in hub._sessions_by_connector["connector"].values()))

            async with client.stream("GET", upstream_url + "/hang") as response:
                assert await anext(response.aiter_raw()) == b"data: 0"
            await asyncio.wait_for(cancelled_upstream.wait(), 5)
            cancelled_upstream.clear()

            # Stop the replica carrying an in-flight stream. It must fail promptly;
            # the next request must use the surviving replica without replaying it.
            stream, session = await hub.open_stream(
                connector_id="connector", method="GET", url=upstream_url + "/hang", headers=[], body=b"",
                timeout_ms=0,
            )
            await asyncio.wait_for(stream.headers_future, 5)
            process = next(process for process in processes if process.pid == session.pid)
            process.terminate()
            await asyncio.wait_for(process.wait(), 5)
            try:
                await collect(stream, session)
            except RemoteConnectorDisconnectedError:
                pass
            else:
                raise AssertionError("Disconnected replica did not fail its in-flight stream")
            await wait_for(lambda: hub.active_session_count("connector") == 1)
            response = await client.get(upstream_url + "/gzip")
            assert response.content == expected

        # Emulate a gateway process restart: discard every hub/session object, keep
        # the surviving connector process running, and rebind the original port.
        stream, session = await hub.open_stream(
            connector_id="connector", method="GET", url=upstream_url + "/hang", headers=[], body=b"",
            timeout_ms=0,
        )
        await asyncio.wait_for(stream.headers_future, 5)
        surviving_processes = [process for process in processes if process.returncode is None]
        server.should_exit = True
        await asyncio.wait_for(server_task, 5)
        try:
            await collect(stream, session)
        except RemoteConnectorDisconnectedError:
            pass
        else:
            raise AssertionError("Gateway shutdown did not fail its in-flight stream")
        await wait_for(lambda: not upstream_tasks)
        await asyncio.sleep(1.5)  # Exercise reconnect attempts while the gateway port is closed.
        assert all(process.returncode is None for process in surviving_processes)
        hub = RemoteConnectorHub()
        server, server_task, restarted_port = await start_gateway(gateway_port)
        assert restarted_port == gateway_port
        await wait_for(lambda: hub.active_session_count("connector") == len(surviving_processes), timeout=15)
        async with httpx.AsyncClient(transport=HttpxTunnelTransport("connector", hub), timeout=5) as client:
            response = await client.post(upstream_url + "/echo", content=b"\xa5" * body_limit)
            assert response.content == b"\xa5" * body_limit
        await wait_for(lambda: all(not session.streams for session in hub._sessions_by_connector["connector"].values()))
        print("Wire smoke passed: gzip, idle SSE, cancellation, 16 MiB roundtrip, 256-stream churn, "
              "replica failover, gateway outage/restart with surviving connector and zero retained streams")
    finally:
        for process in processes:
            if process.returncode is None:
                process.terminate()
        for process in processes:
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(process.wait(), 5)
            if process.returncode is None:
                process.kill()
                await process.wait()
        server.should_exit = True
        await asyncio.wait_for(server_task, 5)
        upstream_server.close()
        await upstream_server.wait_closed()
        pending_upstream = list(upstream_tasks)
        for task in pending_upstream:
            task.cancel()
        await asyncio.gather(*pending_upstream, return_exceptions=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gateway-root", type=Path, required=True)
    parser.add_argument("--connector-binary", type=Path, required=True)
    arguments = parser.parse_args()
    asyncio.run(run(arguments.gateway_root.resolve(), arguments.connector_binary.resolve()))

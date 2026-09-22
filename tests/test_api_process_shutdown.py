"""Real-process API admission behavior during Uvicorn graceful shutdown."""

from __future__ import annotations

import asyncio
import logging
import multiprocessing
import os
import socket
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, closing

import uvicorn
from fastapi import FastAPI

from taskforge.api.request_tracking import ActiveHTTPRequestTracker
from taskforge.api.server import InstrumentedServer


def _unused_port() -> int:
    with closing(socket.socket()) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _serve_shutdown_test(
    port: int,
    ready: multiprocessing.synchronize.Event,
    entered: multiprocessing.synchronize.Event,
    release: multiprocessing.synchronize.Event,
    connection_shutdown: multiprocessing.synchronize.Event,
    second_entered: multiprocessing.synchronize.Event,
) -> None:
    class ShutdownObserver(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            if record.getMessage().startswith("Waiting for connections to close"):
                connection_shutdown.set()

    observer = ShutdownObserver()

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        uvicorn_logger = logging.getLogger("uvicorn.error")
        uvicorn_logger.addHandler(observer)
        try:
            yield
        finally:
            uvicorn_logger.removeHandler(observer)

    app = FastAPI(lifespan=lifespan)

    @app.get("/hold")
    async def hold() -> dict[str, bool]:
        entered.set()
        while not release.is_set():
            await asyncio.sleep(0.01)
        return {"completed": True}

    @app.get("/second")
    async def second() -> dict[str, bool]:
        second_entered.set()
        return {"admitted": True}

    class ReadyServer(InstrumentedServer):
        async def startup(self, sockets: list[socket.socket] | None = None) -> None:
            await super().startup(sockets)
            ready.set()

    server = ReadyServer(
        uvicorn.Config(
            app,
            host="127.0.0.1",
            port=port,
            lifespan="on",
            access_log=False,
            timeout_graceful_shutdown=5,
        )
    )
    server.run()


def test_uvicorn_shutdown_closes_new_and_keep_alive_request_admission() -> None:
    context = multiprocessing.get_context("fork")
    port = _unused_port()
    ready = context.Event()
    entered = context.Event()
    release = context.Event()
    connection_shutdown = context.Event()
    second_entered = context.Event()
    process = context.Process(
        target=_serve_shutdown_test,
        args=(
            port,
            ready,
            entered,
            release,
            connection_shutdown,
            second_entered,
        ),
    )
    process.start()
    connection: socket.socket | None = None
    try:
        assert ready.wait(5)
        connection = socket.create_connection(("127.0.0.1", port), timeout=2)
        connection.sendall(
            b"GET /hold HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n"
        )
        assert entered.wait(5)

        os.kill(process.pid, 15)
        assert connection_shutdown.wait(5)

        with socket.socket() as fresh:
            fresh.settimeout(1)
            assert fresh.connect_ex(("127.0.0.1", port)) != 0

        connection.sendall(
            b"GET /second HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
        )
        release.set()
        response = bytearray()
        connection.settimeout(5)
        while True:
            chunk = connection.recv(4096)
            if not chunk:
                break
            response.extend(chunk)

        process.join(5)
        # Uvicorn 0.52.1 deliberately re-raises the captured SIGTERM after its
        # clean server shutdown, restoring the process's original signal action.
        assert process.exitcode == -15
        assert bytes(response).count(b"HTTP/1.1 200 OK") == 1
        assert not second_entered.is_set()
    finally:
        release.set()
        if connection is not None:
            connection.close()
        if process.is_alive():
            process.kill()
        process.join(5)


def test_active_http_tracker_only_counts_and_waits() -> None:
    async def scenario() -> None:
        tracker = ActiveHTTPRequestTracker()
        await tracker.enter()
        waiter = asyncio.create_task(tracker.wait_empty())
        await asyncio.sleep(0)
        assert tracker.active == 1
        assert not waiter.done()
        await tracker.leave()
        await waiter
        assert tracker.active == 0

    asyncio.run(scenario())

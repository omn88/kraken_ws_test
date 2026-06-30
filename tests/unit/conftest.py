"""Fixtures for unit tests — no network, no Kraken dependency."""

import asyncio
import json

import pytest

from kraken_ws.client import KrakenWSClient


class StubConnection:
    """Async-iterable Kraken connection simulator driven by an internal asyncio.Queue.

    Tests push Kraken-shaped messages via the typed push_* helpers at the right
    moment so there is no race between the read loop and subscription queue
    creation.  Raw push() is still available for malformed-input tests.
    """

    def __init__(self) -> None:
        """Initialise with an empty internal message queue."""
        self._queue: asyncio.Queue = asyncio.Queue()
        self.sent: list[str] = []

    async def push(self, msg: str) -> None:
        """Enqueue a raw string (use for malformed-frame tests)."""
        await self._queue.put(msg)

    async def push_ack(self, channel: str, symbol: str) -> None:
        """Enqueue a subscribe-ack for the given channel and symbol."""
        await self._queue.put(json.dumps({
            "method": "subscribe",
            "result": {"channel": channel, "symbol": symbol, "snapshot": True},
            "success": True,
            "time_in": "2026-01-01T00:00:00Z",
            "time_out": "2026-01-01T00:00:00Z",
        }))

    async def push_unsubscribe_ack(self, channel: str, symbol: str) -> None:
        """Enqueue an unsubscribe-ack for the given channel and symbol."""
        await self._queue.put(json.dumps({
            "method": "unsubscribe",
            "result": {"channel": channel, "symbol": symbol},
            "success": True,
            "time_in": "2026-01-01T00:00:00Z",
            "time_out": "2026-01-01T00:00:00Z",
        }))

    async def push_data(
        self, channel: str, symbol: str, extra: dict | None = None
    ) -> None:
        """Enqueue a snapshot data message for the given channel and symbol."""
        await self._queue.put(json.dumps({
            "channel": channel,
            "type": "snapshot",
            "data": [{"symbol": symbol, "price": 100.0, **(extra or {})}],
        }))

    async def push_heartbeat(self) -> None:
        """Enqueue a heartbeat frame."""
        await self._queue.put(json.dumps({"channel": "heartbeat"}))

    def __aiter__(self) -> "StubConnection":
        """Return self as the async iterator."""
        return self

    async def __anext__(self) -> str:
        """Block until a message is pushed, then yield it."""
        return await self._queue.get()

    async def send(self, data: str) -> None:
        """Record outbound frames without transmitting anything."""
        self.sent.append(data)

    async def close(self) -> None:
        """No-op close."""


@pytest.fixture
def event_loop():
    """Fresh event loop per test — the reader task lives here."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def run(event_loop):
    """Run a coroutine in this test's event loop."""
    return event_loop.run_until_complete


@pytest.fixture
def conn():
    """Fresh StubConnection for each test."""
    return StubConnection()


@pytest.fixture
def client(event_loop, conn):
    """Connected KrakenWSClient backed by the stub; auto-closes after test."""
    c = KrakenWSClient()
    event_loop.run_until_complete(c.connect(_connection=conn))
    yield c
    event_loop.run_until_complete(c.close())

"""Fixtures for unit tests — no network, no Kraken dependency."""

import asyncio

import pytest

from kraken_ws.client import KrakenWSClient


class StubConnection:
    """Async-iterable stub driven by an internal asyncio.Queue.

    Tests push messages via conn.push(raw_json) at the right moment so there
    is no race between the read loop and subscription queue creation.
    """

    def __init__(self) -> None:
        """Initialise with an empty internal message queue."""
        self._queue: asyncio.Queue = asyncio.Queue()
        self.sent: list[str] = []

    async def push(self, msg: str) -> None:
        """Enqueue a raw JSON string to be yielded by the read loop."""
        await self._queue.put(msg)

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

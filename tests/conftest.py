"""Session-scoped fixtures shared by functional and reliability tests.

One WebSocket connection is opened at session start and reused for all live
tests.  Per-test isolation comes from the subscribe/unsubscribe queue lifecycle
in KrakenWSClient, not from opening a new connection per test.

Unit tests (tests/unit/) have their own function-scoped event_loop and run
fixtures in tests/unit/conftest.py which shadow these for that directory.
"""
import asyncio
from collections.abc import Iterator

import pytest  # pylint: disable=import-error

# pylint: disable=redefined-outer-name  # standard pytest fixture pattern

from kraken_ws.client import KrakenWSClient


@pytest.fixture(scope="session")
def session_loop() -> Iterator[asyncio.AbstractEventLoop]:
    """A single event loop shared across all live tests in the session."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def run(session_loop: asyncio.AbstractEventLoop):
    """Run a coroutine to completion on the session event loop."""
    return session_loop.run_until_complete


@pytest.fixture(scope="session")
def live_client(run) -> Iterator[KrakenWSClient]:
    """One shared WebSocket connection for the entire live test session."""
    client = KrakenWSClient()
    run(client.connect())
    yield client
    run(client.close())

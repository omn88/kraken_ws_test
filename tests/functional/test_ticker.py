"""Functional tests for the ticker channel."""

import asyncio

import pytest

from kraken_ws.client import KrakenWSClient
from tests.constants import SYMBOLS


async def _first_message(symbol: str) -> dict:
    """Connect, subscribe to ticker, return the first message, then close."""
    async with KrakenWSClient() as client:
        await client.subscribe("ticker", [symbol])
        msg = await client.next_message("ticker", symbol, timeout=10)
        await client.unsubscribe("ticker", [symbol])
    return msg


@pytest.mark.parametrize("symbol", SYMBOLS)
def test_ticker_snapshot_received(symbol: str) -> None:
    """Subscribing to ticker produces a snapshot for the requested symbol."""
    msg = asyncio.run(_first_message(symbol))
    assert msg["channel"] == "ticker"
    assert msg["type"] == "snapshot"
    assert msg["data"][0]["symbol"] == symbol


@pytest.mark.parametrize("symbol", SYMBOLS)
def test_ticker_bid_ask_spread(symbol: str) -> None:
    """Best bid is strictly below best ask."""
    msg = asyncio.run(_first_message(symbol))
    data = msg["data"][0]
    assert data["bid"] < data["ask"]


@pytest.mark.parametrize("symbol", SYMBOLS)
def test_ticker_schema(symbol: str) -> None:
    """Ticker snapshot contains all expected fields with correct types."""
    msg = asyncio.run(_first_message(symbol))
    data = msg["data"][0]
    assert isinstance(data["bid"], float)
    assert isinstance(data["bid_qty"], float)
    assert isinstance(data["ask"], float)
    assert isinstance(data["ask_qty"], float)
    assert isinstance(data["last"], float)
    assert isinstance(data["volume"], float)
    assert isinstance(data["vwap"], float)
    assert isinstance(data["low"], float)
    assert isinstance(data["high"], float)
    assert isinstance(data["change_pct"], float)
    assert isinstance(data["timestamp"], str)

"""Functional tests for the ohlc channel."""

import asyncio

import pytest

from kraken_ws.client import KrakenWSClient
from tests.constants import SYMBOLS


async def _first_message(symbol: str) -> dict:
    """Connect, subscribe to ohlc (1-minute candles), return the snapshot, then close."""
    async with KrakenWSClient() as client:
        await client.subscribe("ohlc", [symbol], interval=1)
        msg = await client.next_message("ohlc", symbol, timeout=10)
        await client.unsubscribe("ohlc", [symbol])
    return msg


@pytest.mark.parametrize("symbol", SYMBOLS)
def test_ohlc_snapshot_received(symbol: str) -> None:
    """Subscribing to ohlc produces a snapshot with at least one candle."""
    msg = asyncio.run(_first_message(symbol))
    assert msg["channel"] == "ohlc"
    assert msg["type"] == "snapshot"
    assert len(msg["data"]) >= 1
    assert msg["data"][0]["symbol"] == symbol


@pytest.mark.parametrize("symbol", SYMBOLS)
def test_ohlc_candle_ordering(symbol: str) -> None:
    """Candles in the snapshot are ordered chronologically by interval_begin."""
    msg = asyncio.run(_first_message(symbol))
    timestamps = [candle["interval_begin"] for candle in msg["data"]]
    assert timestamps == sorted(timestamps)


@pytest.mark.parametrize("symbol", SYMBOLS)
def test_ohlc_schema(symbol: str) -> None:
    """OHLC candle contains all expected fields with correct types."""
    msg = asyncio.run(_first_message(symbol))
    candle = msg["data"][0]
    assert isinstance(candle["open"], float)
    assert isinstance(candle["high"], float)
    assert isinstance(candle["low"], float)
    assert isinstance(candle["close"], float)
    assert isinstance(candle["volume"], float)
    assert isinstance(candle["vwap"], float)
    assert isinstance(candle["trades"], int)
    assert isinstance(candle["interval"], int)
    assert isinstance(candle["interval_begin"], str)

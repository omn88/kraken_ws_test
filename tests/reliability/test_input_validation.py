"""Reliability tests: server-side validation of subscribe requests."""
import asyncio

from kraken_ws.client import KrakenWSClient


async def _subscribe_bad_channel() -> dict:
    """Subscribe to a non-existent channel and return the server ack."""
    async with KrakenWSClient() as client:
        acks = await client.subscribe("nonexistent_channel", ["BTC/USD"])
    return acks[0]


async def _subscribe_bad_symbol() -> dict:
    """Subscribe to ticker with an invalid symbol and return the server ack."""
    async with KrakenWSClient() as client:
        acks = await client.subscribe("ticker", ["INVALID/PAIR"])
    return acks[0]


async def _subscribe_valid_and_return_ack() -> dict:
    """Subscribe to ticker with a valid symbol and return the ack."""
    async with KrakenWSClient() as client:
        acks = await client.subscribe("ticker", ["BTC/USD"])
        await client.unsubscribe("ticker", ["BTC/USD"])
    return acks[0]


def test_invalid_channel_name_returns_error() -> None:
    """Subscribing to a non-existent channel returns a structured error, not a disconnect."""
    ack = asyncio.run(_subscribe_bad_channel())
    assert ack["success"] is False
    assert "error" in ack
    assert isinstance(ack["error"], str) and ack["error"]
    assert ack.get("method") == "subscribe"


def test_invalid_symbol_returns_error() -> None:
    """Subscribing with an unknown symbol returns a structured error, not a disconnect."""
    ack = asyncio.run(_subscribe_bad_symbol())
    assert ack["success"] is False
    assert "error" in ack
    assert isinstance(ack["error"], str) and ack["error"]
    assert ack.get("method") == "subscribe"


def test_subscribe_ack_shape_and_time_fields() -> None:
    """A successful subscribe ack carries method, result, success, time_in, and time_out."""
    ack = asyncio.run(_subscribe_valid_and_return_ack())
    assert ack["success"] is True
    assert ack["method"] == "subscribe"
    result = ack["result"]
    assert result["channel"] == "ticker"
    assert result["symbol"] == "BTC/USD"
    assert isinstance(ack["time_in"], str) and ack["time_in"]
    assert isinstance(ack["time_out"], str) and ack["time_out"]
    # time_out must not precede time_in
    assert ack["time_out"] >= ack["time_in"]

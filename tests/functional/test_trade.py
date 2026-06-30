"""Functional tests for the trade channel.

Trade has no snapshot — messages only arrive on actual market activity.
BTC/USD is used as the most liquid pair; timeout is 30 s to handle brief
quiet periods without flakiness.
"""

import asyncio

from kraken_ws.client import KrakenWSClient

SYMBOL = "BTC/USD"
TIMEOUT = 30


async def _first_trade() -> dict:
    """Connect, subscribe to trade, return the first update, then close."""
    async with KrakenWSClient() as client:
        await client.subscribe("trade", [SYMBOL])
        msg = await client.next_message("trade", SYMBOL, timeout=TIMEOUT)
        await client.unsubscribe("trade", [SYMBOL])
    return msg


def test_trade_update_received() -> None:
    """At least one trade update arrives within the timeout window."""
    msg = asyncio.run(_first_trade())
    assert msg["channel"] == "trade"
    assert msg["type"] == "update"
    assert len(msg["data"]) >= 1


def test_trade_schema() -> None:
    """Trade update contains all expected fields with correct types."""
    msg = asyncio.run(_first_trade())
    trade = msg["data"][0]
    assert trade["symbol"] == SYMBOL
    assert isinstance(trade["price"], float)
    assert isinstance(trade["qty"], float)
    assert isinstance(trade["trade_id"], int)
    assert isinstance(trade["timestamp"], str)


def test_trade_side_and_ord_type() -> None:
    """Trade side is buy or sell; ord_type is a non-empty string."""
    msg = asyncio.run(_first_trade())
    trade = msg["data"][0]
    assert trade["side"] in {"buy", "sell"}
    assert isinstance(trade["ord_type"], str) and trade["ord_type"]

"""Functional tests for the trade channel.

Trade has no snapshot — messages only arrive on actual market activity.
BTC/USD is used as the most liquid pair; timeout is 30 s to handle brief
quiet periods without flakiness.
"""
from kraken_ws.client import KrakenWSClient

SYMBOL = "BTC/USD"
TIMEOUT = 30


def test_trade_update_received(run, live_client: KrakenWSClient) -> None:
    """At least one trade update arrives within the timeout window."""
    run(live_client.subscribe("trade", [SYMBOL]))
    msg = run(live_client.next_message("trade", SYMBOL, timeout=TIMEOUT))
    run(live_client.unsubscribe("trade", [SYMBOL]))
    assert msg["channel"] == "trade"
    assert msg["type"] == "update"
    assert len(msg["data"]) >= 1


def test_trade_schema(run, live_client: KrakenWSClient) -> None:
    """Trade update contains all expected fields with correct types."""
    run(live_client.subscribe("trade", [SYMBOL]))
    msg = run(live_client.next_message("trade", SYMBOL, timeout=TIMEOUT))
    run(live_client.unsubscribe("trade", [SYMBOL]))
    trade = msg["data"][0]
    assert trade["symbol"] == SYMBOL
    assert isinstance(trade["price"], float)
    assert isinstance(trade["qty"], float)
    assert isinstance(trade["trade_id"], int)
    assert isinstance(trade["timestamp"], str)


def test_trade_side_and_ord_type(run, live_client: KrakenWSClient) -> None:
    """Trade side is buy or sell; ord_type is a non-empty string."""
    run(live_client.subscribe("trade", [SYMBOL]))
    msg = run(live_client.next_message("trade", SYMBOL, timeout=TIMEOUT))
    run(live_client.unsubscribe("trade", [SYMBOL]))
    trade = msg["data"][0]
    assert trade["side"] in {"buy", "sell"}
    assert isinstance(trade["ord_type"], str) and trade["ord_type"]

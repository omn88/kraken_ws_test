"""Reliability tests: server-side validation of subscribe requests."""
from datetime import datetime

from kraken_ws.client import KrakenWSClient


def test_invalid_channel_name_returns_error(run, live_client: KrakenWSClient) -> None:
    """Subscribing to a non-existent channel returns a structured error, not a disconnect."""
    acks = run(live_client.subscribe("nonexistent_channel", ["BTC/USD"]))
    ack = acks[0]
    assert ack["success"] is False
    assert "error" in ack
    assert isinstance(ack["error"], str) and ack["error"]
    assert ack.get("method") == "subscribe"


def test_invalid_symbol_returns_error(run, live_client: KrakenWSClient) -> None:
    """Subscribing with an unknown symbol returns a structured error, not a disconnect."""
    acks = run(live_client.subscribe("ticker", ["INVALID/PAIR"]))
    ack = acks[0]
    assert ack["success"] is False
    assert "error" in ack
    assert isinstance(ack["error"], str) and ack["error"]
    assert ack.get("method") == "subscribe"


def test_subscribe_ack_shape_and_time_fields(run, live_client: KrakenWSClient) -> None:
    """A successful subscribe ack carries method, result, success, time_in, and time_out."""
    acks = run(live_client.subscribe("ticker", ["BTC/USD"]))
    run(live_client.unsubscribe("ticker", ["BTC/USD"]))
    ack = acks[0]
    assert ack["success"] is True
    assert ack["method"] == "subscribe"
    result = ack["result"]
    assert result["channel"] == "ticker"
    assert result["symbol"] == "BTC/USD"
    assert isinstance(ack["time_in"], str) and ack["time_in"]
    assert isinstance(ack["time_out"], str) and ack["time_out"]
    t_in = datetime.fromisoformat(ack["time_in"].replace("Z", "+00:00"))
    t_out = datetime.fromisoformat(ack["time_out"].replace("Z", "+00:00"))
    assert t_in <= t_out
    assert (t_out - t_in).total_seconds() < 1.0

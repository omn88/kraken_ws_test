"""Functional tests for the ohlc channel."""
import pytest  # pylint: disable=import-error

from kraken_ws.client import KrakenWSClient
from tests.constants import SYMBOLS


@pytest.mark.parametrize("symbol", SYMBOLS)
def test_ohlc_snapshot_received(symbol: str, run, live_client: KrakenWSClient) -> None:
    """Subscribing to ohlc produces a snapshot with at least one candle."""
    run(live_client.subscribe("ohlc", [symbol], interval=1))
    msg = run(live_client.next_message("ohlc", symbol, timeout=10))
    run(live_client.unsubscribe("ohlc", [symbol]))
    assert msg["channel"] == "ohlc"
    assert msg["type"] == "snapshot"
    assert len(msg["data"]) >= 1
    assert msg["data"][0]["symbol"] == symbol


@pytest.mark.parametrize("symbol", SYMBOLS)
def test_ohlc_candle_ordering(symbol: str, run, live_client: KrakenWSClient) -> None:
    """Candles in the snapshot are ordered chronologically by interval_begin."""
    run(live_client.subscribe("ohlc", [symbol], interval=1))
    msg = run(live_client.next_message("ohlc", symbol, timeout=10))
    run(live_client.unsubscribe("ohlc", [symbol]))
    timestamps = [candle["interval_begin"] for candle in msg["data"]]
    assert len(timestamps) >= 2, (
        f"snapshot returned only {len(timestamps)} candle — ordering test requires at least 2"
    )
    assert timestamps == sorted(timestamps), (
        f"candles not in chronological order: {timestamps}"
    )


@pytest.mark.parametrize("symbol", SYMBOLS)
def test_ohlc_schema(symbol: str, run, live_client: KrakenWSClient) -> None:
    """OHLC candle contains all expected fields with correct types."""
    run(live_client.subscribe("ohlc", [symbol], interval=1))
    msg = run(live_client.next_message("ohlc", symbol, timeout=10))
    run(live_client.unsubscribe("ohlc", [symbol]))
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

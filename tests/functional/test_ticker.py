"""Functional tests for the ticker channel."""
import pytest  # pylint: disable=import-error

from kraken_ws.client import KrakenWSClient
from tests.constants import SYMBOLS


@pytest.mark.parametrize("symbol", SYMBOLS)
def test_ticker_snapshot_received(symbol: str, run, live_client: KrakenWSClient) -> None:
    """Subscribing to ticker produces a snapshot for the requested symbol."""
    run(live_client.subscribe("ticker", [symbol]))
    msg = run(live_client.next_message("ticker", symbol, timeout=10))
    run(live_client.unsubscribe("ticker", [symbol]))
    assert msg["channel"] == "ticker"
    assert msg["type"] == "snapshot"
    assert msg["data"][0]["symbol"] == symbol


@pytest.mark.parametrize("symbol", SYMBOLS)
def test_ticker_bid_ask_spread(symbol: str, run, live_client: KrakenWSClient) -> None:
    """Best bid is strictly below best ask."""
    run(live_client.subscribe("ticker", [symbol]))
    msg = run(live_client.next_message("ticker", symbol, timeout=10))
    run(live_client.unsubscribe("ticker", [symbol]))
    data = msg["data"][0]
    assert data["bid"] < data["ask"]


@pytest.mark.parametrize("symbol", SYMBOLS)
def test_ticker_schema(symbol: str, run, live_client: KrakenWSClient) -> None:
    """Ticker snapshot contains all expected fields with correct types."""
    run(live_client.subscribe("ticker", [symbol]))
    msg = run(live_client.next_message("ticker", symbol, timeout=10))
    run(live_client.unsubscribe("ticker", [symbol]))
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

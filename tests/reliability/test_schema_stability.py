"""Reliability tests: schema consistency across multiple consecutive messages.

Unlike the functional schema tests (which check one message), these collect
N items over time and assert that field names and types do not drift.
"""
from kraken_ws.client import KrakenWSClient

SYMBOL = "BTC/USD"


def _assert_schema_stable(items: list[dict], label: str) -> None:
    """Assert every item has the same field names and types as the first item."""
    reference = {field: type(value) for field, value in items[0].items()}
    for i, item in enumerate(items[1:], start=1):
        for field, expected_type in reference.items():
            assert field in item, (
                f"[{label}] item {i}: field {field!r} present in item 0 but missing later"
            )
            assert isinstance(item[field], expected_type), (
                f"[{label}] item {i}: field {field!r} changed type "
                f"from {expected_type.__name__} to {type(item[field]).__name__}"
            )


def test_ticker_schema_stable_across_symbols(run, live_client: KrakenWSClient) -> None:
    """Ticker snapshot schema is identical for BTC/USD and ETH/USD."""
    run(live_client.subscribe("ticker", ["BTC/USD", "ETH/USD"]))
    btc = run(live_client.next_message("ticker", "BTC/USD", timeout=10))
    eth = run(live_client.next_message("ticker", "ETH/USD", timeout=10))
    run(live_client.unsubscribe("ticker", ["BTC/USD", "ETH/USD"]))
    items = [btc["data"][0], eth["data"][0]]
    assert len(items) == 2
    _assert_schema_stable(items, "ticker")


def test_trade_schema_stable_across_messages(run, live_client: KrakenWSClient) -> None:
    """Trade field names and types are identical across 5 consecutive trade items."""
    run(live_client.subscribe("trade", [SYMBOL]))
    items: list[dict] = []
    while len(items) < 5:
        msg = run(live_client.next_message("trade", SYMBOL, timeout=30))
        items.extend(msg["data"])
    run(live_client.unsubscribe("trade", [SYMBOL]))
    items = items[:5]
    assert len(items) == 5
    _assert_schema_stable(items, "trade")


def test_ohlc_schema_stable_across_messages(run, live_client: KrakenWSClient) -> None:
    """OHLC candle field names and types are identical across 5 consecutive candles."""
    run(live_client.subscribe("ohlc", [SYMBOL], interval=1))
    items: list[dict] = []
    while len(items) < 5:
        msg = run(live_client.next_message("ohlc", SYMBOL, timeout=15))
        items.extend(msg["data"])
    run(live_client.unsubscribe("ohlc", [SYMBOL]))
    items = items[:5]
    assert len(items) == 5
    _assert_schema_stable(items, "ohlc")

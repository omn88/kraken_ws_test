"""Reliability tests: timestamp ordering within a live feed."""
from datetime import datetime

from kraken_ws.client import KrakenWSClient

SYMBOL = "BTC/USD"


def _parse(ts: str) -> datetime:
    """Parse an RFC3339 timestamp string into a timezone-aware datetime."""
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def test_ticker_timestamps_non_decreasing(run, live_client: KrakenWSClient) -> None:
    """Ticker timestamps across consecutive messages are non-decreasing."""
    run(live_client.subscribe("ticker", [SYMBOL]))
    raw = []
    for _ in range(2):
        msg = run(live_client.next_message("ticker", SYMBOL, timeout=30))
        raw.append(msg["data"][0]["timestamp"])
    run(live_client.unsubscribe("ticker", [SYMBOL]))
    parsed = [_parse(ts) for ts in raw]
    for earlier, later in zip(parsed, parsed[1:]):
        assert earlier <= later, f"timestamp went backward: {earlier} > {later}"


def test_trade_timestamps_non_decreasing(run, live_client: KrakenWSClient) -> None:
    """Trade timestamps across consecutive messages are non-decreasing."""
    run(live_client.subscribe("trade", [SYMBOL]))
    raw = []
    for _ in range(3):
        msg = run(live_client.next_message("trade", SYMBOL, timeout=30))
        raw.append(msg["data"][-1]["timestamp"])
    run(live_client.unsubscribe("trade", [SYMBOL]))
    parsed = [_parse(ts) for ts in raw]
    for earlier, later in zip(parsed, parsed[1:]):
        assert earlier <= later, f"timestamp went backward: {earlier} > {later}"

"""Reliability tests: timestamp ordering within a live feed."""
import asyncio
from datetime import datetime

from kraken_ws.client import KrakenWSClient

SYMBOL = "BTC/USD"


def _parse(ts: str) -> datetime:
    """Parse an RFC3339 timestamp string into a timezone-aware datetime."""
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


async def _collect_ticker(count: int) -> list[str]:
    """Subscribe to ticker and collect `count` message timestamps."""
    async with KrakenWSClient() as client:
        await client.subscribe("ticker", [SYMBOL])
        timestamps = []
        for _ in range(count):
            msg = await client.next_message("ticker", SYMBOL, timeout=15)
            timestamps.append(msg["data"][0]["timestamp"])
        await client.unsubscribe("ticker", [SYMBOL])
    return timestamps


async def _collect_trades(count: int) -> list[str]:
    """Subscribe to trade and collect `count` message timestamps."""
    async with KrakenWSClient() as client:
        await client.subscribe("trade", [SYMBOL])
        timestamps = []
        for _ in range(count):
            msg = await client.next_message("trade", SYMBOL, timeout=30)
            # a single message may carry several trades; take the last one
            timestamps.append(msg["data"][-1]["timestamp"])
        await client.unsubscribe("trade", [SYMBOL])
    return timestamps


def test_ticker_timestamps_non_decreasing() -> None:
    """Ticker timestamps across consecutive messages are non-decreasing."""
    raw = asyncio.run(_collect_ticker(3))
    parsed = [_parse(ts) for ts in raw]
    for earlier, later in zip(parsed, parsed[1:]):
        assert earlier <= later, f"timestamp went backward: {earlier} > {later}"


def test_trade_timestamps_non_decreasing() -> None:
    """Trade timestamps across consecutive messages are non-decreasing."""
    raw = asyncio.run(_collect_trades(3))
    parsed = [_parse(ts) for ts in raw]
    for earlier, later in zip(parsed, parsed[1:]):
        assert earlier <= later, f"timestamp went backward: {earlier} > {later}"

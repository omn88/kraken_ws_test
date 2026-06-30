"""Reliability tests: reconnection, keepalive, and concurrent subscriptions."""
import asyncio
from datetime import datetime

from kraken_ws.client import KrakenWSClient

SYMBOL = "BTC/USD"


async def _two_sequential_connections() -> tuple[dict, dict]:
    """Open two separate connections in sequence and return one ticker snapshot each."""
    async with KrakenWSClient() as client:
        await client.subscribe("ticker", [SYMBOL])
        snap1 = await client.next_message("ticker", SYMBOL, timeout=10)
    async with KrakenWSClient() as client:
        await client.subscribe("ticker", [SYMBOL])
        snap2 = await client.next_message("ticker", SYMBOL, timeout=10)
    return snap1, snap2


async def _ping_pong() -> dict:
    """Connect, send application-level ping, return the pong."""
    async with KrakenWSClient() as client:
        return await client.ping()


async def _concurrent_subscriptions() -> tuple[dict, dict]:
    """Subscribe to ticker and ohlc on one socket; return first message from each."""
    async with KrakenWSClient() as client:
        await client.subscribe("ticker", [SYMBOL])
        await client.subscribe("ohlc", [SYMBOL], interval=1)
        ticker_msg = await client.next_message("ticker", SYMBOL, timeout=10)
        ohlc_msg = await client.next_message("ohlc", SYMBOL, timeout=10)
        await client.unsubscribe("ticker", [SYMBOL])
        await client.unsubscribe("ohlc", [SYMBOL])
    return ticker_msg, ohlc_msg


async def _graceful_unsubscribe() -> bool:
    """Subscribe, get snapshot, unsubscribe; return True if queue is gone."""
    async with KrakenWSClient() as client:
        await client.subscribe("ticker", [SYMBOL])
        await client.next_message("ticker", SYMBOL, timeout=10)
        await client.unsubscribe("ticker", [SYMBOL])
        return ("ticker", SYMBOL) not in client._queues


def _assert_ticker_snapshot(snap: dict) -> None:
    """Assert all structural and value constraints on a ticker snapshot dict."""
    assert snap["channel"] == "ticker"
    assert snap["type"] == "snapshot"
    assert len(snap["data"]) == 1
    d = snap["data"][0]
    assert d["symbol"] == SYMBOL
    # Spread is positive
    assert d["bid"] > 0
    assert d["ask"] > 0
    assert d["bid"] < d["ask"], f"bid {d['bid']} >= ask {d['ask']}"
    assert d["bid_qty"] > 0
    assert d["ask_qty"] > 0
    # Last trade price is in a sane range
    assert d["last"] > 0
    # Day range is coherent
    assert d["low"] > 0
    assert d["high"] >= d["low"]
    # Volume and VWAP are non-negative
    assert d["volume"] >= 0
    assert d["vwap"] >= 0
    # Timestamp parses as RFC3339
    ts = datetime.fromisoformat(d["timestamp"].replace("Z", "+00:00"))
    assert ts.tzinfo is not None


def test_reconnect_delivers_fresh_snapshot() -> None:
    """Both connections deliver valid independent ticker snapshots for BTC/USD."""
    snap1, snap2 = asyncio.run(_two_sequential_connections())
    _assert_ticker_snapshot(snap1)
    _assert_ticker_snapshot(snap2)


def test_keepalive_ping_pong() -> None:
    """Application-level ping returns a pong; server processing time is sub-second."""
    pong = asyncio.run(_ping_pong())
    assert pong["method"] == "pong"
    assert "time_in" in pong and "time_out" in pong
    t_in = datetime.fromisoformat(pong["time_in"].replace("Z", "+00:00"))
    t_out = datetime.fromisoformat(pong["time_out"].replace("Z", "+00:00"))
    assert t_in <= t_out, f"time_in {t_in} is after time_out {t_out}"
    assert (t_out - t_in).total_seconds() < 1.0


def test_multiple_subscriptions_demultiplexed() -> None:
    """Ticker and ohlc on one socket each deliver correctly shaped, correctly routed messages."""
    ticker_msg, ohlc_msg = asyncio.run(_concurrent_subscriptions())
    # Ticker-specific checks
    assert ticker_msg["channel"] == "ticker"
    t = ticker_msg["data"][0]
    assert t["symbol"] == SYMBOL
    assert t["bid"] > 0 and t["ask"] > 0
    assert t["bid"] < t["ask"], f"bid {t['bid']} >= ask {t['ask']}"
    assert t["volume"] >= 0
    # OHLC-specific checks
    assert ohlc_msg["channel"] == "ohlc"
    c = ohlc_msg["data"][0]
    assert c["symbol"] == SYMBOL
    assert c["high"] >= c["low"], f"high {c['high']} < low {c['low']}"
    assert c["open"] > 0 and c["close"] > 0
    assert c["volume"] >= 0
    assert c["interval"] == 1
    assert isinstance(c["interval_begin"], str) and c["interval_begin"]


def test_graceful_unsubscribe_drops_queue() -> None:
    """After unsubscribe completes, the (ticker, BTC/USD) queue is removed."""
    queue_gone = asyncio.run(_graceful_unsubscribe())
    assert queue_gone

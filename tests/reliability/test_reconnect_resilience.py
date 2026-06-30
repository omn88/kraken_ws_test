"""Reliability tests: reconnection, keepalive, and concurrent subscriptions."""
from datetime import datetime

from kraken_ws.client import KrakenWSClient

SYMBOL = "BTC/USD"


def _assert_ticker_snapshot(snap: dict) -> None:
    """Assert all structural and value constraints on a ticker snapshot dict."""
    assert snap["channel"] == "ticker"
    assert snap["type"] == "snapshot"
    assert len(snap["data"]) == 1
    d = snap["data"][0]
    assert d["symbol"] == SYMBOL
    assert d["bid"] > 0
    assert d["ask"] > 0
    assert d["bid"] < d["ask"], f"bid {d['bid']} >= ask {d['ask']}"
    assert d["bid_qty"] > 0
    assert d["ask_qty"] > 0
    assert d["last"] > 0
    assert d["low"] > 0
    assert d["high"] >= d["low"]
    assert d["volume"] >= 0
    assert d["vwap"] >= 0
    ts = datetime.fromisoformat(d["timestamp"].replace("Z", "+00:00"))
    assert ts.tzinfo is not None


def test_reconnect_delivers_fresh_snapshot(run) -> None:
    """Two sequential independent connections each deliver a valid ticker snapshot."""
    async def _two_connections():
        snaps = []
        for _ in range(2):
            async with KrakenWSClient() as client:
                await client.subscribe("ticker", [SYMBOL])
                snaps.append(await client.next_message("ticker", SYMBOL, timeout=10))
        return snaps

    snap1, snap2 = run(_two_connections())
    _assert_ticker_snapshot(snap1)
    _assert_ticker_snapshot(snap2)


def test_keepalive_ping_pong(run, live_client: KrakenWSClient) -> None:
    """Application-level ping returns a pong; server processing time is sub-second."""
    pong = run(live_client.ping())
    assert pong["method"] == "pong"
    assert "time_in" in pong and "time_out" in pong
    t_in = datetime.fromisoformat(pong["time_in"].replace("Z", "+00:00"))
    t_out = datetime.fromisoformat(pong["time_out"].replace("Z", "+00:00"))
    assert t_in <= t_out, f"time_in {t_in} is after time_out {t_out}"
    assert (t_out - t_in).total_seconds() < 1.0


def test_multiple_subscriptions_demultiplexed(run, live_client: KrakenWSClient) -> None:
    """Ticker and ohlc on one connection each deliver correctly shaped, correctly routed messages."""
    run(live_client.subscribe("ticker", [SYMBOL]))
    run(live_client.subscribe("ohlc", [SYMBOL], interval=1))
    ticker_msg = run(live_client.next_message("ticker", SYMBOL, timeout=10))
    ohlc_msg = run(live_client.next_message("ohlc", SYMBOL, timeout=10))
    run(live_client.unsubscribe("ticker", [SYMBOL]))
    run(live_client.unsubscribe("ohlc", [SYMBOL]))
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


def test_graceful_unsubscribe_drops_queue(run, live_client: KrakenWSClient) -> None:
    """After unsubscribe completes, the (ticker, BTC/USD) queue is removed."""
    run(live_client.subscribe("ticker", [SYMBOL]))
    run(live_client.next_message("ticker", SYMBOL, timeout=10))
    run(live_client.unsubscribe("ticker", [SYMBOL]))
    assert ("ticker", SYMBOL) not in live_client._queues

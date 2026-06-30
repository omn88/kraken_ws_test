"""Unit tests for KrakenWSClient routing and queue lifecycle.

Tests here exercise our own code, not the Kraken API.  StubConnection and
client lifecycle are managed by fixtures in tests/unit/conftest.py.

Keep this count separate from functional/reliability tests that satisfy the
>=10 / >=3-channel submission requirement.
"""

import asyncio
import json


def test_data_message_routed_to_subscription_queue(run, client, conn) -> None:
    """A data message for a subscribed (channel, symbol) arrives in its queue."""
    run(conn.push_ack("ticker", "BTC/USD"))
    run(client.subscribe("ticker", ["BTC/USD"]))
    run(conn.push_data("ticker", "BTC/USD"))
    msg = run(client.next_message("ticker", "BTC/USD", timeout=2.0))
    assert msg["channel"] == "ticker"
    assert msg["data"][0]["symbol"] == "BTC/USD"


def test_unmatched_message_goes_to_unmatched_queue(run, client, conn) -> None:
    """A data message for a channel/symbol with no subscription lands in _unmatched."""
    run(conn.push_data("ticker", "XBT/USD"))
    run(asyncio.sleep(0))
    assert not client._unmatched.empty()
    msg = run(client._unmatched.get())
    assert msg["channel"] == "ticker"


def test_heartbeat_goes_to_unmatched(run, client, conn) -> None:
    """Heartbeat frames (no data key) route to _unmatched, not a subscription queue."""
    run(conn.push_heartbeat())
    run(asyncio.sleep(0))
    msg = run(client._unmatched.get())
    assert msg["channel"] == "heartbeat"


def test_ack_goes_to_acks_queue(run, client, conn) -> None:
    """Subscribe-ack messages route to _acks and not to subscription queues."""
    run(conn.push_ack("ticker", "BTC/USD"))
    run(asyncio.sleep(0))
    a = run(client._acks.get())
    assert a["method"] == "subscribe"
    assert a["success"]


def test_subscribe_creates_queue_and_sends_frame(run, client, conn) -> None:
    """subscribe() creates a (channel, symbol) queue entry and sends a subscribe frame."""
    run(conn.push_ack("trade", "ETH/USD"))
    run(client.subscribe("trade", ["ETH/USD"]))
    assert ("trade", "ETH/USD") in client._queues
    assert len(conn.sent) == 1
    sent = json.loads(conn.sent[0])
    assert sent["method"] == "subscribe"
    assert sent["params"]["channel"] == "trade"
    assert "ETH/USD" in sent["params"]["symbol"]


def test_unsubscribe_drops_queue_after_ack(run, client, conn) -> None:
    """unsubscribe() removes the (channel, symbol) queue only after the ack arrives."""
    run(conn.push_ack("ticker", "BTC/USD"))
    run(client.subscribe("ticker", ["BTC/USD"]))
    assert ("ticker", "BTC/USD") in client._queues
    run(conn.push_unsubscribe_ack("ticker", "BTC/USD"))
    run(client.unsubscribe("ticker", ["BTC/USD"]))
    assert ("ticker", "BTC/USD") not in client._queues


def test_post_unsubscribe_messages_go_to_unmatched(run, client, conn) -> None:
    """Messages arriving after unsubscribe route to _unmatched, not the dropped queue."""
    run(conn.push_ack("ticker", "BTC/USD"))
    run(client.subscribe("ticker", ["BTC/USD"]))
    run(conn.push_unsubscribe_ack("ticker", "BTC/USD"))
    run(client.unsubscribe("ticker", ["BTC/USD"]))
    run(conn.push_data("ticker", "BTC/USD"))
    run(asyncio.sleep(0))
    assert not client._unmatched.empty()


def test_multiple_symbols_routed_independently(run, client, conn) -> None:
    """Two subscribed symbols on the same channel each receive only their own messages."""
    run(conn.push_ack("ticker", "BTC/USD"))
    run(conn.push_ack("ticker", "ETH/USD"))
    run(client.subscribe("ticker", ["BTC/USD", "ETH/USD"]))
    run(conn.push_data("ticker", "BTC/USD"))
    run(conn.push_data("ticker", "ETH/USD"))
    btc = run(client.next_message("ticker", "BTC/USD", timeout=2.0))
    eth = run(client.next_message("ticker", "ETH/USD", timeout=2.0))
    assert btc["data"][0]["symbol"] == "BTC/USD"
    assert eth["data"][0]["symbol"] == "ETH/USD"


def test_ohlc_snapshot_with_multiple_candles_enqueued_once(run, client, conn) -> None:
    """An OHLC snapshot with N candles for one symbol is queued exactly once, not N times."""
    candles = [
        {"symbol": "BTC/USD", "open": 100.0, "close": 101.0, "interval": i}
        for i in range(10)
    ]
    snapshot = json.dumps({"channel": "ohlc", "type": "snapshot", "data": candles})
    run(conn.push_ack("ohlc", "BTC/USD"))
    run(client.subscribe("ohlc", ["BTC/USD"]))
    run(conn.push(snapshot))
    run(asyncio.sleep(0))
    assert client._queues[("ohlc", "BTC/USD")].qsize() == 1


def test_malformed_json_goes_to_unmatched(run, client, conn) -> None:
    """A frame that is not valid JSON is wrapped and placed in _unmatched."""
    run(conn.push("not valid json {{"))
    run(asyncio.sleep(0))
    assert not client._unmatched.empty()
    item = run(client._unmatched.get())
    assert "_raw" in item
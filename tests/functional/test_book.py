"""Functional tests for the book channel."""
from kraken_ws.book import compute_checksum
from kraken_ws.client import KrakenWSClient

SYMBOL = "BTC/USD"


def test_book_snapshot_received(run, live_client: KrakenWSClient) -> None:
    """Subscribing to book produces a snapshot with bids, asks, and a checksum."""
    run(live_client.subscribe("book", [SYMBOL]))
    msg = run(live_client.next_message("book", SYMBOL, timeout=10))
    run(live_client.unsubscribe("book", [SYMBOL]))
    assert msg["channel"] == "book"
    assert msg["type"] == "snapshot"
    data = msg["data"][0]
    assert data["symbol"] == SYMBOL
    assert len(data["bids"]) > 0
    assert len(data["asks"]) > 0
    assert isinstance(data["checksum"], int)
    assert isinstance(data["timestamp"], str)


def test_book_bid_ask_non_crossing(run, live_client: KrakenWSClient) -> None:
    """Best bid price is strictly below best ask price in the snapshot."""
    run(live_client.subscribe("book", [SYMBOL]))
    msg = run(live_client.next_message("book", SYMBOL, timeout=10))
    run(live_client.unsubscribe("book", [SYMBOL]))
    data = msg["data"][0]
    # Prices are str-typed (preserved for CRC32); cast to float for comparison
    best_bid = max(float(level["price"]) for level in data["bids"])
    best_ask = min(float(level["price"]) for level in data["asks"])
    assert best_bid < best_ask, f"book crossed: best bid {best_bid} >= best ask {best_ask}"


def test_book_snapshot_checksum(run, live_client: KrakenWSClient) -> None:
    """CRC32 checksum recomputed from snapshot levels matches the value Kraken sent."""
    run(live_client.subscribe("book", [SYMBOL]))
    msg = run(live_client.next_message("book", SYMBOL, timeout=10))
    run(live_client.unsubscribe("book", [SYMBOL]))
    data = msg["data"][0]
    bids = [(lvl["price"], lvl["qty"]) for lvl in data["bids"]]
    asks = [(lvl["price"], lvl["qty"]) for lvl in data["asks"]]
    computed = compute_checksum(bids, asks)
    assert computed == data["checksum"], (
        f"checksum mismatch: computed {computed}, server sent {data['checksum']}"
    )

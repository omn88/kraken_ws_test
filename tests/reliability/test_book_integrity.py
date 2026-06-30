"""Reliability tests: order book integrity over a bounded incremental window.

Full continuous reconstruction (gap detection, resync after disconnect) is
out of scope for this take-home — see NOTES.txt. What is tested here:
- The snapshot checksum can be independently reproduced from its price levels.
- Applying up to 20 incremental updates to a local book state and re-verifying
  the checksum after each confirms that Kraken's updates are internally
  consistent over a short, bounded window.

Prices and quantities in book messages are string-typed (preserved from the
raw JSON by the client) so that trailing zeros are not lost before CRC32
computation (e.g. JSON "0.00005100" → Python float 5.1e-05 would be wrong).
"""
from kraken_ws.book import compute_checksum
from kraken_ws.client import KrakenWSClient

SYMBOL = "BTC/USD"
UPDATE_WINDOW = 20


def test_book_checksum_holds_after_updates(run, live_client: KrakenWSClient) -> None:
    """Checksum remains valid across a bounded window of incremental book updates."""
    run(live_client.subscribe("book", [SYMBOL]))

    # Seed local book from snapshot; keys and values are str to preserve precision
    snap = run(live_client.next_message("book", SYMBOL, timeout=10))
    assert snap["type"] == "snapshot"
    data = snap["data"][0]
    bids: dict[str, str] = {lvl["price"]: lvl["qty"] for lvl in data["bids"]}
    asks: dict[str, str] = {lvl["price"]: lvl["qty"] for lvl in data["asks"]}

    # Verify snapshot checksum before applying any updates
    initial = compute_checksum(list(bids.items()), list(asks.items()))
    assert initial == data["checksum"], (
        f"snapshot checksum mismatch: computed {initial}, server sent {data['checksum']}"
    )

    # Apply updates and re-verify after each
    for i in range(UPDATE_WINDOW):
        msg = run(live_client.next_message("book", SYMBOL, timeout=10))
        assert msg["type"] == "update", f"expected update at step {i}, got {msg['type']}"
        upd = msg["data"][0]
        for lvl in upd["bids"]:
            if float(lvl["qty"]) == 0:
                bids.pop(lvl["price"], None)
            else:
                bids[lvl["price"]] = lvl["qty"]
        for lvl in upd["asks"]:
            if float(lvl["qty"]) == 0:
                asks.pop(lvl["price"], None)
            else:
                asks[lvl["price"]] = lvl["qty"]
        computed = compute_checksum(list(bids.items()), list(asks.items()))
        assert computed == upd["checksum"], (
            f"checksum mismatch at update {i}: computed {computed}, server sent {upd['checksum']}"
        )

    run(live_client.unsubscribe("book", [SYMBOL]))

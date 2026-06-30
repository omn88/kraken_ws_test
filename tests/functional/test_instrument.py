"""Functional tests for the instrument channel.

The instrument channel's documented subscribe schema defines no symbol parameter
(unlike ticker, trade, ohlc, and book which all require one) — it always returns
a full snapshot of all tradeable pairs. Its data field is a dict
({"pairs": [...], "assets": [...]}) rather than a list, so messages route to
_unmatched in the client rather than to a (channel, symbol) queue. This test
uses a dedicated fresh connection to avoid polluting the shared session client's
_unmatched queue. The channel may also send "update" messages after the initial
snapshot when reference data changes; none were observed during testing, but the
helper only needs the snapshot so this does not affect correctness.
"""
import asyncio
import json
from typing import Any

from kraken_ws.client import KrakenWSClient
from tests.constants import SYMBOLS


async def _fetch_instrument_pairs(timeout: float = 30.0) -> list[dict[str, Any]]:
    """Subscribe to the instrument channel and return the full pairs list."""
    async with KrakenWSClient() as client:
        await client._ws.send(
            json.dumps({"method": "subscribe", "params": {"channel": "instrument"}})
        )
        # One subscribe ack arrives before the snapshot data
        await asyncio.wait_for(client._acks.get(), timeout=timeout)
        # At most a status message precedes the snapshot; bounded drain is safe
        for _ in range(5):
            msg = await asyncio.wait_for(client._unmatched.get(), timeout=timeout)
            if msg.get("channel") == "instrument" and msg.get("type") == "snapshot":
                return msg["data"]["pairs"]
    raise AssertionError("instrument snapshot not received within expected message window")


def test_instrument_symbols_are_active(run) -> None:
    """Confirms the suite's hardcoded symbol list refers to live, active trading pairs."""
    pairs = run(_fetch_instrument_pairs())
    by_symbol = {pair["symbol"]: pair for pair in pairs}
    for symbol in SYMBOLS:
        assert symbol in by_symbol, (
            f"{symbol} not found in instrument pairs list — symbol may have been renamed or removed"
        )
        status = by_symbol[symbol]["status"]
        assert status != "delisted", (
            f"{symbol} has status {status!r}; expected an active status (online/post_only/cancel_only)"
        )

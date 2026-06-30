import asyncio
import json
from typing import Any, Optional

import websockets


class KrakenWSClient:
    """Thin async WebSocket client for the Kraken v2 public API.

    One socket, one background read loop, per-subscription queues.
    Supports use as an async context manager: async with KrakenWSClient() as c.
    """

    def __init__(self, url: str = "wss://ws.kraken.com/v2") -> None:
        """Initialise the client without opening a connection."""
        self.url = url
        self._ws: Any = None
        self._queues: dict[tuple[str, str], asyncio.Queue] = {}
        self._unmatched: asyncio.Queue = asyncio.Queue()
        self._acks: asyncio.Queue = asyncio.Queue()
        self._reader_task: Optional[asyncio.Task] = None

    async def connect(self, _connection: Any = None) -> None:
        """Open the WebSocket and start the background read loop.

        Inject _connection (any async-iterable with a send() coroutine) to
        bypass the real network — used by unit tests in tests/unit/.

        Retries up to 3 times with exponential backoff on HTTP 429 so that
        running the full suite (many connections in quick succession) does not
        fail due to Kraken's per-IP connection rate limit.
        """
        if _connection is not None:
            self._ws = _connection
        else:
            for attempt in range(3):
                try:
                    self._ws = await websockets.connect(self.url)
                    break
                except websockets.exceptions.InvalidStatus as exc:
                    if "429" not in str(exc) or attempt == 2:
                        raise
                    await asyncio.sleep(2 ** attempt)  # 1 s, then 2 s
        self._reader_task = asyncio.create_task(self._read_loop())

    async def subscribe(
        self, channel: str, symbols: list[str], **extra_params: Any
    ) -> list[dict[str, Any]]:
        """Create per-symbol queues, send subscribe, drain all acks, and return them.

        Extra keyword arguments are forwarded into the params object, e.g.
        interval=1 for ohlc or depth=10 for book.  Callers can inspect the
        returned acks to check the success field (e.g. for invalid-input tests).
        """
        for symbol in symbols:
            self._queues[(channel, symbol)] = asyncio.Queue()
        params: dict[str, Any] = {"channel": channel, "symbol": symbols}
        params.update(extra_params)
        await self._ws.send(json.dumps({"method": "subscribe", "params": params}))
        acks: list[dict[str, Any]] = []
        for _ in symbols:
            acks.append(await asyncio.wait_for(self._acks.get(), timeout=10.0))
        return acks

    async def unsubscribe(self, channel: str, symbols: list[str]) -> None:
        """Send unsubscribe, drain acks, then drop the subscription queues.

        After this returns, messages for (channel, symbol) route to _unmatched,
        which lets tests assert no further data arrives for the feed.
        """
        await self._ws.send(
            json.dumps(
                {
                    "method": "unsubscribe",
                    "params": {"channel": channel, "symbol": symbols},
                }
            )
        )
        for _ in symbols:
            await asyncio.wait_for(self._acks.get(), timeout=10.0)
        for symbol in symbols:
            self._queues.pop((channel, symbol), None)

    async def ping(self) -> dict[str, Any]:
        """Send an application-level ping and return the pong response.

        Distinct from the WS-protocol ping/pong — this is Kraken's own
        {"method": "ping"} keepalive mechanism.  The response carries
        time_in / time_out fields useful for latency checks.
        """
        await self._ws.send(json.dumps({"method": "ping"}))
        return await asyncio.wait_for(self._acks.get(), timeout=10.0)

    async def next_message(
        self, channel: str, symbol: str, timeout: float = 10.0
    ) -> dict[str, Any]:
        """Return the next message from the given subscription queue."""
        return await asyncio.wait_for(
            self._queues[(channel, symbol)].get(), timeout=timeout
        )

    async def __aenter__(self) -> "KrakenWSClient":
        """Connect on entry — enables `async with KrakenWSClient() as client`."""
        await self.connect()
        return self

    async def __aexit__(self, *_: object) -> None:
        """Close on exit."""
        await self.close()

    async def close(self) -> None:
        """Cancel the read loop and close the WebSocket connection."""
        if self._reader_task is not None:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass
        if self._ws is not None:
            await self._ws.close()

    async def _read_loop(self) -> None:
        """Decode every incoming frame and route it to the right queue.

        Routing rules (see CLAUDE.md §11):
        - 'method' key present  → _acks (subscribe/unsubscribe confirmations)
        - 'data' list with symbol fields → (channel, symbol) queue per unique
          symbol; whole message goes into _unmatched if no key matches
        - anything else (heartbeat, status, unknown) → _unmatched
        """
        async for raw in self._ws:
            try:
                msg: dict[str, Any] = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                await self._unmatched.put({"_raw": raw})
                continue

            # Book prices and quantities must preserve trailing zeros for CRC32
            # (e.g. JSON "0.00005100" → Python float 5.1e-05 loses the zeros).
            # Re-parse with parse_float=str so the checksum can be recomputed.
            if msg.get("channel") == "book":
                msg = json.loads(raw, parse_float=str)

            if "method" in msg:
                await self._acks.put(msg)
                continue

            data = msg.get("data")
            if isinstance(data, list):
                channel: str = msg.get("channel", "")
                seen: set[str] = set()
                routed_any = False
                for item in data:
                    symbol = item.get("symbol")
                    if symbol and symbol not in seen:
                        seen.add(symbol)
                        key = (channel, symbol)
                        if key in self._queues:
                            await self._queues[key].put(msg)
                            routed_any = True
                if not routed_any:
                    await self._unmatched.put(msg)
            else:
                await self._unmatched.put(msg)

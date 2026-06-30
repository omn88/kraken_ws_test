"""Curated, static test data — deliberately not fetched live.

A regression suite should run identically every time.  Hardcoding a small
list of liquid pairs avoids runtime drift and prevents hangs on thinly-traded
symbols with no recent activity.
"""

SYMBOLS: list[str] = ["BTC/USD", "ETH/USD"]
CHANNELS: list[str] = ["ticker", "trade", "ohlc"]

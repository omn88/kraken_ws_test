"""Order book utilities for the Kraken v2 book channel."""
import zlib


def _fmt(value: str) -> str:
    """Format a price or qty string for checksum: remove decimal point, strip leading zeros."""
    s = value.replace(".", "")
    return s.lstrip("0") or "0"


def compute_checksum(
    bids: list[tuple[str, str]],
    asks: list[tuple[str, str]],
) -> int:
    """Compute the Kraken v2 book CRC32 checksum.

    Asks (lowest price first) are concatenated before bids (highest price
    first).  Each price and qty has its decimal point removed and leading
    zeros stripped before concatenation.
    """
    top_asks = sorted(asks, key=lambda x: float(x[0]))[:10]
    top_bids = sorted(bids, key=lambda x: float(x[0]), reverse=True)[:10]
    parts: list[str] = []
    for price, qty in top_asks:
        parts.append(_fmt(price))
        parts.append(_fmt(qty))
    for price, qty in top_bids:
        parts.append(_fmt(price))
        parts.append(_fmt(qty))
    return zlib.crc32("".join(parts).encode())

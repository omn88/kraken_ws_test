# Kraken WebSocket Test Suite

Pytest test suite for Kraken's public WebSocket API v2 (`wss://ws.kraken.com/v2`).
No Kraken account or API key required.

**41 tests** across four channels (ticker, trade, ohlc, book):
- 18 functional tests — schema, types, and value constraints per channel
- 13 reliability tests — timestamp ordering, book checksum integrity, reconnect recovery, keepalive, concurrent subscription demultiplexing, graceful unsubscribe, invalid input handling, ack latency, and schema stability
- 10 unit tests — client message routing and queue lifecycle (no network)

See `NOTES.txt` for test strategy, design trade-offs, and what was deliberately left out of scope.

## Dependencies

- Python 3.12+
- `websockets==16.0`
- `pytest==9.1.1`

## Running with Docker

```bash
docker build -t kraken-ws-tests .
docker run --rm kraken-ws-tests
```

The container runs the full suite against the live Kraken endpoint.
An active internet connection is required.

Alternatively, `run.sh` at the project root automates both steps — it builds the image only if it doesn't already exist locally, then runs it:

```bash
bash run.sh
```

Use `bash run.sh` rather than `./run.sh` so the command works regardless of whether the executable bit survived archive extraction.

## Running locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest tests/ -v
```

To run only unit tests (no network):

```bash
pytest tests/unit/ -v
```

To run only live-API tests:

```bash
pytest tests/functional/ tests/reliability/ -v
```

## Project layout

```
kraken_ws/
  client.py      # KrakenWSClient — one socket, one read loop, per-subscription queues
  book.py        # CRC32 checksum computation for the book channel
tests/
  unit/          # Tests of KrakenWSClient itself — no network, fully deterministic
  functional/    # Per-channel schema and value tests (ticker, trade, ohlc, book)
  reliability/   # Non-functional invariants: ordering, integrity, recovery, validation
```

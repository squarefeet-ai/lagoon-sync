# Lagoon Sync

Lagoon Sync is a resumable and auto-tuning **S3-to-S3** replicator with manifest support.
It ingest Parquet manifest files that list object keys, stream objects directly from a source S3-compatible bucket to a destination bucket, and continuously tunes concurrency with an AIMD-style congestion controller.

## Key Features

| Capability | Detail |
|------------|--------|
| **Resumable & Idempotent** | Transfer state is persisted in an LMDB database; interruptions can be resumed without re‑copying. |
| **Manifest‑Driven** | Scans only for new `.parquet` manifests under a configurable prefix—no full bucket listings. |
| **Auto‑Tuning Throughput** | Optional congestion controller adjusts concurrency in real time based on RTT, goodput, and error rate. |
| **Memory‑Efficient Streaming** | Objects are streamed—never fully loaded into memory. |
| **Graceful Shutdown** | Handles `SIGINT`/`SIGTERM`, finishing in‑flight transfers before exit. |

## Quick Start

```bash
# 1. Install uv (https://github.com/astral-sh/uv)
curl -Ls https://astral.sh/uv/install.sh | bash

# 2. Clone the repository
git clone https://github.com/squarefeet-ai/lagoon-sync
cd lagoon-sync

# 3. Install runtime dependencies
uv pip install .

# 4. Copy and edit environment variables
cp .env.example .env
nano .env  # fill in credentials & bucket names

# 5. Run the synchronizer
uv pip sync
lagoon-sync --help  # view CLI options
lagoon-sync  # start the pipeline
```
> **Tip**: For long-running transfers, execute inside a `tmux` session.

## Configuration

All settings are supplied via environment variables and command-line flags.

| Variable | Purpose |
|----------|---------|
| `LAGOON_SOURCE_*` | Endpoint, credentials, region & bucket for the **source**. |
| `LAGOON_DESTINATION_*` | Endpoint, credentials, region & bucket for the **destination**. |

*See `.env.example` for the full list*.

CLI flags let you override operational parameters (data-dir, max-concurrency, logging level, etc.). For example:

```bash
lagoon-sync \
  --data-dir /var/lib/lagoon-sync \
  --max-concurrency 512 \
  --log-level DEBUG
```

## Development & Testing

```bash
# Install dev extras
uv pip install -e ".[dev]"

# Run the test suite (unit + e2e)
uv run pytest
```

## How It Works

1. **Manifest Discovery**: finds new manifest files under `manifest_prefix` (default: `manifest/`) in the source bucket.
2. **Key Extraction**: loads keys via **Polars**` Parquet scanner (no local downloads).
3. **Progress Filtering**: consults LMDB to skip keys already marked `COMPLETED`.
4. **Concurrent Transfers**: spawns async workers, each copying one object with streaming GET/PUT.
5. **Congestion Control**: optional controller adjusts a dynamic semaphore every `controller_interval_s` seconds.
6. **State Tracking**: each outcome (`COMPLETED`/`FAILED`) is recorded for fault-tolerant restarts,

Lagoon Sync therefore achieves high throughput while remaining resilient and minimal in operational overheads.
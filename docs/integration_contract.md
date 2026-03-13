# Integration Contract

This document defines the production contract exposed by `crypto-leader-rotation` to downstream strategy systems.

The upstream project publishes a monthly `core_major` live pool and exposes it through:

1. local build artifacts under `data/output/`
2. versioned and current objects in GCS
3. a lightweight Firestore summary document

## Canonical Downstream Files

### `live_pool_legacy.json`

This is the most convenient file for older downstream scripts that expect a direct symbol mapping.

Schema:

```json
{
  "as_of_date": "2026-03-13",
  "pool_size": 5,
  "symbols": {
    "TRXUSDT": {"base_asset": "TRX"},
    "ETHUSDT": {"base_asset": "ETH"},
    "BCHUSDT": {"base_asset": "BCH"},
    "NEARUSDT": {"base_asset": "NEAR"},
    "LTCUSDT": {"base_asset": "LTC"}
  }
}
```

Contract notes:

- `as_of_date`: the production snapshot date in `YYYY-MM-DD`
- `pool_size`: number of symbols currently published
- `symbols`: mapping from `SYMBOLUSDT` to `{base_asset}`
- keys are the production `core_major` pool unless explicitly overridden during build

### `live_pool.json`

This file contains both the ordered list and the symbol mapping:

```json
{
  "as_of_date": "2026-03-13",
  "pool_size": 5,
  "symbols": ["TRXUSDT", "ETHUSDT", "BCHUSDT", "NEARUSDT", "LTCUSDT"],
  "symbol_map": {
    "TRXUSDT": {"base_asset": "TRX"},
    "ETHUSDT": {"base_asset": "ETH"},
    "BCHUSDT": {"base_asset": "BCH"},
    "NEARUSDT": {"base_asset": "NEAR"},
    "LTCUSDT": {"base_asset": "LTC"}
  }
}
```

## Firestore Contract

Collection and document defaults:

- collection: `strategy`
- document: `CRYPTO_LEADER_ROTATION_LIVE_POOL`

Payload example:

```json
{
  "as_of_date": "2026-03-13",
  "mode": "core_major",
  "version": "2026-03-13-core_major",
  "pool_size": 5,
  "symbols": ["TRXUSDT", "ETHUSDT", "BCHUSDT", "NEARUSDT", "LTCUSDT"],
  "symbol_map": {
    "TRXUSDT": {"base_asset": "TRX"},
    "ETHUSDT": {"base_asset": "ETH"},
    "BCHUSDT": {"base_asset": "BCH"},
    "NEARUSDT": {"base_asset": "NEAR"},
    "LTCUSDT": {"base_asset": "LTC"}
  },
  "storage_prefix": "gs://example-bucket/crypto-leader-rotation/releases/2026-03-13-core_major",
  "current_prefix": "gs://example-bucket/crypto-leader-rotation/current",
  "live_pool_legacy_uri": "gs://example-bucket/crypto-leader-rotation/current/live_pool_legacy.json",
  "live_pool_uri": "gs://example-bucket/crypto-leader-rotation/current/live_pool.json",
  "latest_universe_uri": "gs://example-bucket/crypto-leader-rotation/current/latest_universe.json",
  "latest_ranking_uri": "gs://example-bucket/crypto-leader-rotation/current/latest_ranking.csv",
  "versioned_live_pool_legacy_uri": "gs://example-bucket/crypto-leader-rotation/releases/2026-03-13-core_major/live_pool_legacy.json",
  "generated_at": "2026-03-13T13:00:00+00:00",
  "source_project": "crypto-leader-rotation"
}
```

The Firestore document intentionally excludes the full ranking CSV. Downstream readers should only rely on the summary fields above.

## GCS Path Layout

Versioned release objects:

```text
gs://<bucket>/crypto-leader-rotation/releases/<YYYY-MM-DD-mode>/latest_universe.json
gs://<bucket>/crypto-leader-rotation/releases/<YYYY-MM-DD-mode>/latest_ranking.csv
gs://<bucket>/crypto-leader-rotation/releases/<YYYY-MM-DD-mode>/live_pool.json
gs://<bucket>/crypto-leader-rotation/releases/<YYYY-MM-DD-mode>/live_pool_legacy.json
```

Current pointers:

```text
gs://<bucket>/crypto-leader-rotation/current/latest_universe.json
gs://<bucket>/crypto-leader-rotation/current/latest_ranking.csv
gs://<bucket>/crypto-leader-rotation/current/live_pool.json
gs://<bucket>/crypto-leader-rotation/current/live_pool_legacy.json
```

## Recommended Downstream Read Priority

1. Read Firestore `strategy/CRYPTO_LEADER_ROTATION_LIVE_POOL`
2. If Firestore is unavailable, read the synchronized `live_pool_legacy.json`
3. If both fail, fall back to the downstream script's static universe

## Downstream Pseudocode

```python
def load_trend_pool():
    payload = try_read_firestore("strategy", "CRYPTO_LEADER_ROTATION_LIVE_POOL")
    if payload and isinstance(payload.get("symbol_map"), dict) and payload["symbol_map"]:
        return payload["symbol_map"]

    legacy = try_read_local_json("live_pool_legacy.json")
    if legacy and isinstance(legacy.get("symbols"), dict) and legacy["symbols"]:
        return legacy["symbols"]

    return STATIC_TREND_UNIVERSE
```

## Rollback Strategy

Preferred rollback:

1. choose the previous version under `gs://<bucket>/crypto-leader-rotation/releases/`
2. copy its four artifacts back onto the `current/` prefix
3. update the Firestore summary document so `version`, `as_of_date`, and URIs point to that release

The downstream consumer contract does not need to change during rollback.

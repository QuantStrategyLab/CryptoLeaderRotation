# Validation Status

This document tracks the current frozen production decision and what still remains experimental.

## Frozen Default

The repository is now frozen around this production default:

- `Production v1`
- data source: `Binance Spot only`
- live universe mode: `core_major`
- publish cadence: `monthly`
- default outputs:
  - `latest_universe.json`
  - `latest_ranking.csv`
  - `live_pool.json`
  - `live_pool_legacy.json`

This is the only path that should be treated as the formal production baseline.

The external-data branch is retained, but only as:

- research
- comparison
- quality hardening
- experimental validation

It is not enabled by default and it is not part of the default production publish chain.

## Current Strategy Validation Snapshot

Latest available baseline in this repository:

- research universe mode: `broad_liquid`
- production live mode: `core_major`
- production data source: `Binance-only`
- publish cadence: `monthly`
- live pool version: `2026-03-13-core_major`

Current live pool:

- `TRXUSDT`
- `ETHUSDT`
- `BCHUSDT`
- `NEARUSDT`
- `LTCUSDT`

Latest research summary for `final_score`:

- CAGR: `35.51%`
- Annualized Volatility: `63.96%`
- Sharpe: `0.7966`
- Max Drawdown: `-76.77%`
- Turnover: `11.99`

Latest walk-forward summary:

- windows: `31`
- mean H30 Precision@N: `0.2167`
- mean H60 Precision@N: `0.2217`
- mean H90 Precision@N: `0.2231`
- mean H30 Leader Capture: `0.1843`
- mean H60 Leader Capture: `0.1833`
- mean H90 Leader Capture: `0.1935`
- mean window Sharpe: `0.8810`
- mean window Turnover: `16.91`

Interpretation:

- the project is now usable as an upstream production pool publisher
- research and walk-forward validation are real and reproducible
- the default production path is intentionally frozen around Binance-only stability
- current priority should remain monthly refresh discipline and stable contract publishing rather than further production-path experimentation

## Publish Chain Validation Completed

Validated in-repo:

- `scripts/build_live_pool.py` produces the default `Production v1` live output
- `scripts/publish_release.py --dry-run` builds a correct production release manifest
- `scripts/write_release_heartbeat.py` writes a small logs-branch heartbeat file
- GitHub Actions workflow YAML parses correctly
- release versioning, GCS object keys, and Firestore payload layout are consistent
- `release_manifest.json` and heartbeat payloads are internally consistent

Validated artifacts:

- `data/output/latest_universe.json`
- `data/output/latest_ranking.csv`
- `data/output/live_pool.json`
- `data/output/live_pool_legacy.json`
- `data/output/release_manifest.json`
- `data/output/heartbeat/monthly/<version>.json`

## External Data Preparation Validation Completed

Validated in-repo:

- provider abstraction exists
- pre-Binance and alternate-exchange merge logic exists
- duplicate-date resolution and source priority work in mock tests
- merged series remain monotonic and deduplicated
- optional market-cap metadata loader works in mock mode

Current external-data conclusion:

- external-data is now close enough to remain worth tracking
- the best experimental profile is `external_data_core_only_no_doge`
- but it still does not win clearly enough across the full `30 / 60 / 90` walk-forward objective set to replace `Production v1`
- therefore external-data remains experimental only

## Not Finished Yet

The following are intentionally not complete yet:

1. real GCS upload validation with production credentials
2. real Firestore write validation with production credentials
3. first successful GitHub Actions `workflow_dispatch` run in the hosted environment
4. rollback drill using a previous published version
5. promotion of external-data from experimental to production, if future validation justifies it
6. model-quality improvement work
7. LightGBM environment hardening on all target runtimes

## Pending Optimization Items

These are known next-step improvements, but they are not blockers for the current upstream publishing scope:

1. improve leader capture and precision in the broader research universe
2. revisit rule / ML blending after the universe split settles
3. tighten download ranking further to reduce very young hype-asset overrepresentation
4. validate `core_major` stability across more monthly snapshots
5. continue comparing Binance-only and external-history builds in the experimental track only
6. add a non-destructive rollback helper for release manifests and current pointers

## Release Blockers Still Remaining

Before relying on the monthly publisher in production, the remaining must-do checks are:

1. configure repository Secrets / Variables correctly
2. verify service-account permissions for Storage and Firestore
3. run one real publish from GitHub Actions
4. confirm the `logs` branch heartbeat push succeeds
5. test downstream consumer reading the published contract without changing strategy logic

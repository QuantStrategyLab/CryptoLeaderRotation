# Operator Runbook

## Scope

This runbook covers the production release path for `crypto-leader-rotation`.

Primary production outputs:

- `data/output/latest_universe.json`
- `data/output/latest_ranking.csv`
- `data/output/live_pool.json`
- `data/output/live_pool_legacy.json`
- `data/output/release_manifest.json`

Primary publish targets:

- GCS current pointers under `crypto-leader-rotation/current`
- GCS versioned release objects under `crypto-leader-rotation/releases/<version>`
- Firestore `strategy/CRYPTO_LEADER_ROTATION_LIVE_POOL`

## Research Path Vs Production Path

Production path:

- `scripts/download_history.py`
- `scripts/build_live_pool.py`
- `scripts/validate_release_contract.py`
- `scripts/publish_release.py`
- `scripts/write_release_heartbeat.py`

Research-only / non-publish path:

- `scripts/run_research_backtest.py`
- `scripts/run_walkforward_validation.py`
- `scripts/compare_external_data.py`
- `scripts/sweep_external_data_profiles.py`
- `scripts/run_monthly_shadow_build.py`

Rules:

- Do not treat shadow outputs, external-data experiments, or research summaries as publish-ready production artifacts.
- Only `core_major` build outputs that pass contract validation should be published to downstream systems.
- If a manual run uses `--as-of-date` for historical investigation, treat it as replay unless you intentionally publish with `--allow-stale`.

## Standard Monthly Flow

1. Refresh or verify local data:

```bash
.venv/bin/python scripts/download_history.py --top-liquid 90 --force-exchange-info
```

2. Build the production pool:

```bash
.venv/bin/python scripts/build_live_pool.py --universe-mode core_major
```

3. Run explicit contract validation:

```bash
.venv/bin/python scripts/validate_release_contract.py --mode core_major --expected-pool-size 5
```

4. Run publish preflight without external writes:

```bash
.venv/bin/python scripts/publish_release.py --dry-run --mode core_major
```

5. Only after steps 1-4 pass, run the real publish path through the workflow or a controlled manual execution.

## Preflight Checklist

- `requirements-lock.txt` is present and matches the intended release dependency set.
- Local artifacts are non-empty and pass `scripts/validate_release_contract.py`.
- `live_pool.json`, `live_pool_legacy.json`, and `release_manifest.json` agree on `as_of_date`, `version`, `mode`, `pool_size`, and `source_project`.
- `GCP_PROJECT_ID`, `GCS_BUCKET`, `FIRESTORE_COLLECTION`, and `FIRESTORE_DOCUMENT` are set correctly for real publish.
- Historical backfills use `--allow-stale` explicitly; do not silently publish stale artifacts.

## Common Failure Modes

### Missing or malformed artifacts

Symptoms:

- `validate_release_contract.py` exits non-zero
- `publish_release.py` fails during preflight

Actions:

- Re-run `scripts/build_live_pool.py`
- Inspect `data/output/latest_universe.json`, `live_pool.json`, `live_pool_legacy.json`, and `latest_ranking.csv`
- Confirm `pool_size`, `symbols`, `symbol_map`, `version`, and `source_project` are present and aligned

### Stale artifacts

Symptoms:

- Contract validation reports outputs are older than the allowed age window

Actions:

- Prefer rebuilding from refreshed raw data
- If you are intentionally replaying an older month, use `--allow-stale` and record that the run is historical
- Do not treat stale output as a healthy production release

### Missing publish configuration

Symptoms:

- Preflight errors for `GCP_PROJECT_ID`, `GCS_BUCKET`, Firestore collection, or Firestore document

Actions:

- Fix the missing variable or workflow secret first
- Re-run `scripts/publish_release.py --dry-run --mode core_major`
- Only retry real publish after dry-run is clean

### Empty pool or ranking mismatch

Symptoms:

- `pool_size` mismatch
- live pool symbols not found in ranking
- live pool symbols are not a subset of latest universe

Actions:

- Stop the release
- Review the latest build inputs and `config/default.yaml`
- Rebuild and validate before any downstream sync

## Minimal Rollback

Use rollback only when the newest publish is clearly bad or malformed.

1. Identify the last known good version from:

- Firestore document history
- GCS `crypto-leader-rotation/releases/<version>/`
- the last good `data/output/release_manifest.json`

2. Restore the four canonical artifacts from that version into `data/output/`:

- `latest_universe.json`
- `latest_ranking.csv`
- `live_pool.json`
- `live_pool_legacy.json`

3. Validate the restored payload locally:

```bash
.venv/bin/python scripts/validate_release_contract.py --mode core_major --expected-pool-size 5
```

4. Regenerate the manifest and verify publish preflight:

```bash
.venv/bin/python scripts/publish_release.py --dry-run --mode core_major --allow-stale
```

5. Re-publish the restored version to GCS / Firestore through the controlled release path.

Rollback note:

- `--allow-stale` is expected during rollback because the restored version is historical.
- Record the rollback version and reason in the operator log or release notes.

## Post-Release Checks

- Confirm Firestore `strategy/CRYPTO_LEADER_ROTATION_LIVE_POOL` contains the expected `version`, `mode`, `symbols`, and `source_project`.
- Confirm GCS current pointers and versioned objects exist for the same version.
- Confirm downstream consumers are reading the new version without falling back to degraded sources.

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "output"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a reporting-only monthly review package from monthly build outputs."
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory containing monthly build outputs.",
    )
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(str(path))
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_track_summary(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(str(path))
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
    if not rows:
        raise ValueError(f"CSV is empty: {path}")
    return rows


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def build_review_inputs(output_dir: Path | str) -> dict[str, Any]:
    root = Path(output_dir)
    summary_path = root / "monthly_shadow_build_summary.json"
    live_pool_path = root / "live_pool.json"
    manifest_path = root / "release_manifest.json"
    track_summary_path = root / "shadow_candidate_tracks" / "track_summary.csv"

    return {
        "summary": load_json(summary_path),
        "live_pool": load_json(live_pool_path),
        "manifest": load_json(manifest_path),
        "track_rows": load_track_summary(track_summary_path),
        "paths": {
            "monthly_shadow_build_summary": str(summary_path),
            "live_pool": str(live_pool_path),
            "release_manifest": str(manifest_path),
            "track_summary": str(track_summary_path),
        },
    }


def derive_warnings(inputs: dict[str, Any]) -> list[str]:
    summary = inputs["summary"]
    live_pool = inputs["live_pool"]
    manifest = inputs["manifest"]
    track_rows = inputs["track_rows"]

    warnings: list[str] = []
    as_of_date = str(summary.get("as_of_date", live_pool.get("as_of_date", ""))).strip()
    version = str(live_pool.get("version", "")).strip()
    mode = str(live_pool.get("mode", "")).strip()

    if not as_of_date:
        warnings.append("missing upstream as_of_date")
    if not version:
        warnings.append("missing live_pool version")
    if not mode:
        warnings.append("missing live_pool mode")

    if str(manifest.get("as_of_date", "")).strip() != as_of_date:
        warnings.append("release_manifest as_of_date does not match monthly summary")
    if str(manifest.get("version", "")).strip() != version:
        warnings.append("release_manifest version does not match live_pool version")
    if str(manifest.get("mode", "")).strip() != mode:
        warnings.append("release_manifest mode does not match live_pool mode")

    track_map = {row.get("track_id", ""): row for row in track_rows}
    for track_id in ("official_baseline", "challenger_topk_60"):
        row = track_map.get(track_id)
        if row is None:
            warnings.append(f"missing track summary row for {track_id}")
            continue
        if str(row.get("last_as_of_date", "")).strip() != as_of_date:
            warnings.append(f"{track_id} last_as_of_date does not match monthly summary")

    if not live_pool.get("symbols"):
        warnings.append("live_pool symbols are empty")
    if _safe_int(live_pool.get("pool_size")) != len(live_pool.get("symbols", [])):
        warnings.append("live_pool pool_size does not match symbols length")

    return warnings


def build_review_questions() -> list[str]:
    return [
        "Does the official baseline publish chain look internally consistent for this month?",
        "Are the shadow candidate track artifacts current and aligned with the same as_of_date?",
        "Is there any operational mismatch between the monthly summary, live pool, and release manifest?",
        "Before the next monthly cycle, what operator follow-up items should be tracked explicitly?",
    ]


def build_review_payload(inputs: dict[str, Any]) -> dict[str, Any]:
    summary = inputs["summary"]
    live_pool = inputs["live_pool"]
    manifest = inputs["manifest"]
    track_rows = inputs["track_rows"]
    track_map = {row.get("track_id", ""): row for row in track_rows}
    official_track = track_map.get("official_baseline", {})
    challenger_track = track_map.get("challenger_topk_60", {})

    warnings = derive_warnings(inputs)
    official_baseline = summary.get("official_baseline", {})

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "as_of_date": str(summary.get("as_of_date", live_pool.get("as_of_date", ""))).strip(),
        "status": "warning" if warnings else "ok",
        "official_baseline": {
            "profile": str(official_baseline.get("profile", official_track.get("profile_name", "baseline_blended_rank"))),
            "version": str(official_baseline.get("version", live_pool.get("version", ""))),
            "mode": str(official_baseline.get("mode", live_pool.get("mode", ""))),
            "pool_size": _safe_int(official_baseline.get("pool_size", live_pool.get("pool_size", 0))),
            "symbols": list(live_pool.get("symbols", [])),
            "source_project": str(live_pool.get("source_project", "")),
        },
        "publish": {
            "dry_run": bool(manifest.get("dry_run")),
            "publish_enabled": bool(manifest.get("publish_enabled")),
            "release_prefix": str(manifest.get("release_prefix", "")),
            "current_prefix": str(manifest.get("current_prefix", "")),
            "firestore_collection": str(manifest.get("firestore", {}).get("collection", "")),
            "firestore_document": str(manifest.get("firestore", {}).get("document", "")),
        },
        "tracks": {
            "official_baseline": {
                "release_count": _safe_int(official_track.get("release_count", 0)),
                "first_as_of_date": str(official_track.get("first_as_of_date", "")),
                "last_as_of_date": str(official_track.get("last_as_of_date", "")),
                "candidate_status": str(official_track.get("candidate_status", "")),
                "release_index_path": str(official_track.get("release_index_path", "")),
            },
            "challenger_topk_60": {
                "release_count": _safe_int(challenger_track.get("release_count", 0)),
                "first_as_of_date": str(challenger_track.get("first_as_of_date", "")),
                "last_as_of_date": str(challenger_track.get("last_as_of_date", "")),
                "candidate_status": str(challenger_track.get("candidate_status", "")),
                "release_index_path": str(challenger_track.get("release_index_path", "")),
            },
        },
        "warnings": warnings,
        "operator_checklist": [
            "Run `make monthly-shadow-build` before generating the review package.",
            "Confirm `live_pool.json`, `release_manifest.json`, and `track_summary.csv` all point to the same month.",
            "Review warning lines before any manual publish or communication follow-up.",
            "Keep the official baseline as the only production reference unless a separate governance process approves a change.",
        ],
        "review_questions": build_review_questions(),
        "source_files": inputs["paths"],
    }


def render_review_markdown(payload: dict[str, Any]) -> str:
    official = payload["official_baseline"]
    publish = payload["publish"]
    tracks = payload["tracks"]
    warning_lines = "\n".join(f"- {item}" for item in payload["warnings"]) if payload["warnings"] else "- none"
    checklist_lines = "\n".join(f"{idx}. {item}" for idx, item in enumerate(payload["operator_checklist"], start=1))
    symbols = ", ".join(official["symbols"]) if official["symbols"] else "n/a"

    return f"""# Monthly Review

Generated: {payload['generated_at_utc']}

## Current release status

- Status: {payload['status']}
- As-of date: {payload['as_of_date']}
- Official profile: {official['profile']}
- Official version / mode: {official['version']} / {official['mode']}
- Official pool size: {official['pool_size']}
- Official symbols: {symbols}
- Source project: {official['source_project']}

## Publish summary

- dry_run: {publish['dry_run']}
- publish_enabled: {publish['publish_enabled']}
- release_prefix: {publish['release_prefix'] or 'n/a'}
- current_prefix: {publish['current_prefix'] or 'n/a'}
- firestore target: {publish['firestore_collection'] or 'n/a'} / {publish['firestore_document'] or 'n/a'}

## Track coverage

- official_baseline: releases={tracks['official_baseline']['release_count']} first={tracks['official_baseline']['first_as_of_date']} last={tracks['official_baseline']['last_as_of_date']} status={tracks['official_baseline']['candidate_status']}
- challenger_topk_60: releases={tracks['challenger_topk_60']['release_count']} first={tracks['challenger_topk_60']['first_as_of_date']} last={tracks['challenger_topk_60']['last_as_of_date']} status={tracks['challenger_topk_60']['candidate_status']}

## Warnings

{warning_lines}

## Operator checklist

{checklist_lines}
"""


def render_review_prompt(payload: dict[str, Any]) -> str:
    questions = "\n".join(f"{idx}. {item}" for idx, item in enumerate(payload["review_questions"], start=1))
    warnings = "\n".join(f"- {item}" for item in payload["warnings"]) if payload["warnings"] else "- none"
    return f"""Monthly release review prompt

Context:
- This package is reporting-only.
- official_baseline remains the production reference.
- challenger_topk_60 remains a shadow candidate artifact.
- No automatic switch or publish decision should be inferred from this file alone.

Current month:
- as_of_date: {payload['as_of_date']}
- status: {payload['status']}
- official version: {payload['official_baseline']['version']}
- official mode: {payload['official_baseline']['mode']}
- official symbols: {", ".join(payload['official_baseline']['symbols']) or 'n/a'}

Warnings:
{warnings}

Questions:
{questions}
"""


def write_outputs(payload: dict[str, Any], output_dir: Path | str) -> dict[str, Path]:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    review_md_path = root / "monthly_review.md"
    review_json_path = root / "monthly_review.json"
    review_prompt_path = root / "monthly_review_prompt.md"

    review_md_path.write_text(render_review_markdown(payload), encoding="utf-8")
    review_json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    review_prompt_path.write_text(render_review_prompt(payload), encoding="utf-8")
    return {
        "review_markdown": review_md_path,
        "review_json": review_json_path,
        "review_prompt": review_prompt_path,
    }


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir).resolve()
    inputs = build_review_inputs(output_dir)
    payload = build_review_payload(inputs)
    outputs = write_outputs(payload, output_dir)

    print(f"status={payload['status']}")
    print(f"as_of_date={payload['as_of_date']}")
    print(f"review_markdown={outputs['review_markdown']}")
    print(f"review_json={outputs['review_json']}")
    print(f"review_prompt={outputs['review_prompt']}")


if __name__ == "__main__":
    main()

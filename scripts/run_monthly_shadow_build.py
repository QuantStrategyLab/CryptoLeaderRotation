#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.build_shadow_candidate_tracks import build_shadow_candidate_tracks
from src.config import load_config
from src.pipeline import build_live_pool_outputs
from src.publish import run_release_publish
from src.utils import date_to_str, get_logger, write_json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the monthly official baseline artifacts plus the shadow candidate tracks."
    )
    parser.add_argument("--config", default="config/default.yaml", help="Path to the YAML config file.")
    parser.add_argument("--as-of-date", default=None, help="Optional historical snapshot date for the official baseline build.")
    parser.add_argument("--universe-mode", default=None, help="Optional official baseline universe mode override.")
    parser.add_argument(
        "--shadow-universe-mode",
        default=None,
        help="Optional research universe mode override for the shadow candidate tracks.",
    )
    parser.add_argument(
        "--skip-publish-dry-run",
        action="store_true",
        help="Skip the additive publish dry-run validation step.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logger = get_logger("run_monthly_shadow_build")
    config = load_config(args.config)
    as_of_date = pd.Timestamp(args.as_of_date) if args.as_of_date else None

    baseline_result = build_live_pool_outputs(
        config,
        as_of_date=as_of_date,
        universe_mode=args.universe_mode,
    )
    publish_result = None
    if not args.skip_publish_dry_run:
        publish_result = run_release_publish(
            config,
            mode=args.universe_mode,
            dry_run=True,
        )

    shadow_result = build_shadow_candidate_tracks(
        config_path=args.config,
        universe_mode=args.shadow_universe_mode,
        logger=logger,
    )
    shadow_summary = shadow_result["summary_table"]

    output_dir = config["paths"].output_dir
    summary_path = output_dir / "monthly_shadow_build_summary.json"
    payload = {
        "as_of_date": date_to_str(baseline_result["as_of_date"]),
        "official_baseline": {
            "profile": "baseline_blended_rank",
            "source_track": "official_baseline",
            "candidate_status": "official_reference",
            "version": str(baseline_result["live_payload"]["version"]),
            "mode": str(baseline_result["live_payload"]["mode"]),
            "pool_size": int(baseline_result["live_payload"]["pool_size"]),
            "live_pool_path": str(output_dir / "live_pool.json"),
            "live_pool_legacy_path": str(output_dir / "live_pool_legacy.json"),
            "publish_manifest_path": str(publish_result["manifest_path"]) if publish_result is not None else None,
        },
        "shadow_candidate_tracks": {
            "root_dir": str(shadow_result["output_root"]),
            "track_summary_path": str(shadow_result["summary_path"]),
            "tracks": shadow_summary.to_dict("records"),
        },
    }
    write_json(summary_path, payload)

    print(f"as_of_date={payload['as_of_date']}")
    print(f"official_live_pool={payload['official_baseline']['live_pool_path']}")
    print(f"official_publish_manifest={payload['official_baseline']['publish_manifest_path']}")
    for row in shadow_summary.to_dict("records"):
        print(
            f"shadow_track={row['track_id']} profile={row['profile_name']} "
            f"index={row['release_index_path']}"
        )
    print(f"summary_path={summary_path}")


if __name__ == "__main__":
    main()

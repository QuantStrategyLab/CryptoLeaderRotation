#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils import write_json


def build_run_url(explicit_run_url: str | None = None) -> str | None:
    if explicit_run_url:
        return explicit_run_url
    run_id = os.getenv("GITHUB_RUN_ID")
    repository = os.getenv("GITHUB_REPOSITORY")
    server_url = os.getenv("GITHUB_SERVER_URL", "https://github.com")
    if run_id and repository:
        return f"{server_url}/{repository}/actions/runs/{run_id}"
    return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write a small release heartbeat JSON for the logs branch.")
    parser.add_argument("--manifest", default="data/output/release_manifest.json", help="Path to release_manifest.json.")
    parser.add_argument("--output-dir", required=True, help="Root directory where monthly/<version>.json will be written.")
    parser.add_argument("--run-id", default=None, help="Optional workflow run id override.")
    parser.add_argument("--run-url", default=None, help="Optional workflow run URL override.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    firestore_payload = manifest["firestore"]["payload"]
    version = str(manifest["version"])
    output_path = Path(args.output_dir) / "monthly" / f"{version}.json"

    run_id = args.run_id or os.getenv("GITHUB_RUN_ID")
    run_url = build_run_url(args.run_url)
    heartbeat = {
        "version": version,
        "as_of_date": firestore_payload["as_of_date"],
        "mode": firestore_payload["mode"],
        "pool_size": firestore_payload["pool_size"],
        "symbols": firestore_payload["symbols"],
        "storage_prefix": firestore_payload["storage_prefix"],
        "generated_at": firestore_payload["generated_at"],
        "workflow_run_id": str(run_id) if run_id is not None else None,
        "workflow_run_url": run_url,
    }
    write_json(output_path, heartbeat)
    print(output_path)


if __name__ == "__main__":
    main()

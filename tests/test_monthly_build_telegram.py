from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "run_monthly_build_telegram.py"
SPEC = importlib.util.spec_from_file_location("monthly_build_telegram", SCRIPT_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC is not None and SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class MonthlyBuildTelegramTests(unittest.TestCase):
    def write_fixture_files(
        self,
        root: Path,
        *,
        include_manifest: bool = True,
        include_shadow_outputs: bool = True,
    ) -> None:
        output_dir = root / "data" / "output"
        shadow_dir = output_dir / "shadow_candidate_tracks"
        shadow_dir.mkdir(parents=True, exist_ok=True)

        summary = {
            "as_of_date": "2026-03-13",
            "official_baseline": {
                "profile": "baseline_blended_rank",
                "version": "2026-03-13-core_major",
                "mode": "core_major",
                "pool_size": 5,
            },
        }
        live_pool = {
            "as_of_date": "2026-03-13",
            "version": "2026-03-13-core_major",
            "mode": "core_major",
            "pool_size": 5,
        }
        with (output_dir / "live_pool.json").open("w", encoding="utf-8") as handle:
            json.dump(live_pool, handle)
        with (output_dir / "release_status_summary.json").open("w", encoding="utf-8") as handle:
            json.dump(
                {
                    "official_release": {
                        "as_of_date": "2026-03-13",
                        "version": "2026-03-13-core_major",
                        "mode": "core_major",
                        "pool_size": 5,
                    },
                    "validation": {"ok": True, "manifest_present": include_manifest, "age_days": 1, "errors": [], "warnings": []},
                },
                handle,
            )
        if include_manifest:
            with (output_dir / "release_manifest.json").open("w", encoding="utf-8") as handle:
                json.dump({"dry_run": True, "version": "2026-03-13-core_major"}, handle)
        if include_shadow_outputs:
            with (output_dir / "monthly_shadow_build_summary.json").open("w", encoding="utf-8") as handle:
                json.dump(summary, handle)
            with (shadow_dir / "track_summary.csv").open("w", encoding="utf-8") as handle:
                handle.write(
                    "track_id,profile_name,target_mode,source_track,candidate_status,release_count,first_as_of_date,last_as_of_date,release_index_path\n"
                    "official_baseline,baseline_blended_rank,blended_rank_pct,official_baseline,official_reference,64,2020-12-31,2026-03-13,official/release_index.csv\n"
                    "challenger_topk_60,challenger_topk_60,future_topk_label_60,shadow_candidate,shadow_candidate,64,2020-12-31,2026-03-13,challenger/release_index.csv\n"
                )

    def test_build_health_payload_reports_ok_for_complete_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            self.write_fixture_files(root)
            payload = MODULE.build_health_payload(root / "data" / "output")

        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["as_of_date"], "2026-03-13")
        self.assertTrue(payload["manifest"]["present"])
        self.assertEqual(payload["shadow_tracks"]["challenger_topk_60"]["release_count"], 64)
        self.assertEqual(payload["warnings"], [])

    def test_build_health_payload_warns_when_manifest_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            self.write_fixture_files(root, include_manifest=False)
            payload = MODULE.build_health_payload(root / "data" / "output")
            message = MODULE.format_message(payload)

        self.assertEqual(payload["status"], "warning")
        self.assertIn("missing release_manifest.json", payload["warnings"])
        self.assertIn("status: warning", message)
        self.assertIn("challenger_topk_60", message)

    def test_build_health_payload_allows_release_status_only_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            self.write_fixture_files(root, include_shadow_outputs=False)
            payload = MODULE.build_health_payload(root / "data" / "output")
            message = MODULE.format_message(payload)

        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["as_of_date"], "2026-03-13")
        self.assertFalse(payload["shadow_tracks"]["official_baseline"]["available"])
        self.assertIn("shadow: not_generated_in_this_run", message)


if __name__ == "__main__":
    unittest.main()

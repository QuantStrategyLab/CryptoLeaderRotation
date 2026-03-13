#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_config
from src.pipeline import build_live_pool_outputs
from src.utils import get_logger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the latest live universe, ranking, and pool exports. Defaults to Production v1 (Binance-only + core_major)."
    )
    parser.add_argument("--config", default="config/default.yaml", help="Path to the YAML config file.")
    parser.add_argument("--as-of-date", default=None, help="Optional historical snapshot date for live build.")
    parser.add_argument("--universe-mode", default=None, help="Optional universe mode override, e.g. core_major.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    as_of_date = pd.Timestamp(args.as_of_date) if args.as_of_date else None
    logger = get_logger("build_live_pool")

    result = build_live_pool_outputs(config, as_of_date=as_of_date, universe_mode=args.universe_mode)
    logger.info("Live pool built for %s", result["as_of_date"].date())
    logger.info(
        "Universe mode: %s | Training window: %s -> %s | linear=%s | ml=%s",
        result["universe_mode"],
        result["train_start_date"].date(),
        result["train_end_date"].date(),
        result["linear_backend"],
        result["ml_backend"],
    )
    logger.info("Export payload:\n%s", result["live_payload"])


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_config
from src.external_data import load_optional_market_cap_metadata, merge_histories_with_external


def build_history_frame(rows: list[tuple[str, float]]) -> pd.DataFrame:
    records = []
    for date_str, close in rows:
        close = float(close)
        records.append(
            {
                "date": date_str,
                "open": close * 0.99,
                "high": close * 1.01,
                "low": close * 0.98,
                "close": close,
                "volume": 1000.0,
                "quote_volume": close * 1000.0,
            }
        )
    return pd.DataFrame(records)


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_root = Path(tmp_dir)
        pre_dir = tmp_root / "data" / "external" / "pre_binance"
        alt_dir = tmp_root / "data" / "external" / "alternate_exchange"
        market_cap_dir = tmp_root / "data" / "external" / "market_cap"
        pre_dir.mkdir(parents=True, exist_ok=True)
        alt_dir.mkdir(parents=True, exist_ok=True)
        market_cap_dir.mkdir(parents=True, exist_ok=True)

        symbol = "ETHUSDT"
        binance_history = build_history_frame(
            [
                ("2020-01-03", 100.0),
                ("2020-01-04", 101.0),
                ("2020-01-06", 104.0),
            ]
        )
        pre_binance_history = build_history_frame(
            [
                ("2019-12-30", 95.0),
                ("2019-12-31", 96.0),
                ("2020-01-01", 97.0),
                ("2020-01-02", 98.0),
                ("2020-01-03", 99.0),
            ]
        )
        alternate_history = build_history_frame(
            [
                ("2020-01-02", 98.5),
                ("2020-01-05", 103.0),
            ]
        )
        pre_binance_history.to_csv(pre_dir / f"{symbol}.csv", index=False)
        alternate_history.to_csv(alt_dir / f"{symbol}.csv", index=False)
        pd.DataFrame(
            [
                {"symbol": "ETHUSDT", "market_cap_usd": 1_000_000_000},
                {"symbol": "SOLUSDT", "market_cap_usd": 750_000_000},
            ]
        ).to_csv(market_cap_dir / "market_cap_snapshot.csv", index=False)

        config = load_config(
            "config/default.yaml",
            overrides={
                "external_data": {
                    "enabled": True,
                    "use_market_cap_filter": True,
                    "quality_gate": {
                        "enabled": False,
                    },
                    "providers": {
                        "pre_binance_local": {
                            "enabled": True,
                            "directory": str(pre_dir.relative_to(tmp_root)),
                        },
                        "alternate_exchange_local": {
                            "enabled": True,
                            "directory": str(alt_dir.relative_to(tmp_root)),
                        },
                        "market_cap_local": {
                            "enabled": True,
                            "path": str((market_cap_dir / "market_cap_snapshot.csv").relative_to(tmp_root)),
                        },
                    },
                }
            },
            project_root=tmp_root,
        )

        histories, summary = merge_histories_with_external({symbol: binance_history}, config)
        merged = histories[symbol]
        market_cap = load_optional_market_cap_metadata(config)

        payload = {
            "merged_rows": int(len(merged)),
            "merged_dates": [str(date.date()) for date in pd.to_datetime(merged["date"])],
            "merged_sources": merged[["date", "data_source"]].assign(
                date=lambda df: df["date"].dt.strftime("%Y-%m-%d")
            ).to_dict("records"),
            "summary": summary.to_dict("records"),
            "market_cap_rows": int(len(market_cap)),
            "market_cap_symbols": market_cap["symbol"].tolist(),
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

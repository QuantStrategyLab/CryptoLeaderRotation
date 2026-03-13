from __future__ import annotations

from typing import Any

import pandas as pd

from .utils import date_to_str, write_json


def export_latest_universe(panel: pd.DataFrame, output_dir: str | Any, as_of_date: pd.Timestamp) -> dict[str, Any]:
    """Export the latest dynamic universe to JSON."""
    snapshot = panel.xs(as_of_date, level="date")
    symbols = sorted(snapshot.index[snapshot["in_universe"]].tolist())
    payload = {"as_of_date": date_to_str(as_of_date), "symbols": symbols}
    write_json(output_dir / "latest_universe.json", payload)
    return payload


def export_latest_ranking(panel: pd.DataFrame, output_dir: str | Any, as_of_date: pd.Timestamp) -> pd.DataFrame:
    """Export the latest ranking cross section to CSV."""
    snapshot = panel.xs(as_of_date, level="date").copy()
    snapshot = snapshot.loc[snapshot["in_universe"] | snapshot["selected_flag"]].copy()
    snapshot["as_of_date"] = date_to_str(as_of_date)
    snapshot["symbol"] = snapshot.index
    columns = [
        "as_of_date",
        "symbol",
        "rule_score",
        "linear_score",
        "ml_score",
        "final_score",
        "regime",
        "confidence",
        "selected_flag",
        "current_rank",
    ]
    exported = snapshot[columns].sort_values("final_score", ascending=False).reset_index(drop=True)
    exported.to_csv(output_dir / "latest_ranking.csv", index=False)
    return exported


def export_live_pool(
    ranking_snapshot: pd.DataFrame,
    metadata: pd.DataFrame,
    output_dir: str | Any,
    as_of_date: pd.Timestamp,
    pool_size: int,
    save_legacy: bool = True,
) -> dict[str, Any]:
    """Export the latest live pool in both simple and legacy-compatible forms."""
    selected = ranking_snapshot.sort_values("final_score", ascending=False).head(pool_size).copy()
    symbols = selected.index.tolist()
    metadata_indexed = metadata.set_index("symbol")
    symbol_map = {
        symbol: {"base_asset": str(metadata_indexed.loc[symbol, "base_asset"])}
        for symbol in symbols
        if symbol in metadata_indexed.index
    }
    payload = {
        "as_of_date": date_to_str(as_of_date),
        "pool_size": len(symbols),
        "symbols": symbols,
        "symbol_map": symbol_map,
    }
    write_json(output_dir / "live_pool.json", payload)

    if save_legacy:
        legacy_payload = {
            "as_of_date": date_to_str(as_of_date),
            "pool_size": len(symbols),
            "symbols": symbol_map,
        }
        write_json(output_dir / "live_pool_legacy.json", legacy_payload)
    return payload


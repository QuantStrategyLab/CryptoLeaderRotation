from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def build_labels(panel: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    """Build forward-return and cross-sectional leader labels for each horizon."""
    label_cfg = config["labels"]
    panel = panel.copy().sort_index()
    horizons = [int(horizon) for horizon in label_cfg["horizons"]]
    future_top_k = int(label_cfg["future_top_k"])

    for horizon in horizons:
        future_return_column = f"future_return_{horizon}"
        future_rank_column = f"future_rank_pct_{horizon}"
        future_top_column = f"future_topk_label_{horizon}"

        panel[future_return_column] = (
            panel.groupby(level="symbol")["close"].shift(-horizon) / panel["close"] - 1.0
        )
        panel[future_rank_column] = np.nan
        panel[future_top_column] = np.nan

        for date, group in panel.groupby(level="date"):
            valid_mask = group["in_universe"] & group[future_return_column].notna()
            if valid_mask.sum() == 0:
                continue
            future_returns = group.loc[valid_mask, future_return_column]
            rank_pct = future_returns.rank(pct=True, ascending=True, method="average")
            rank_pos = future_returns.rank(ascending=False, method="first")
            panel.loc[rank_pct.index, future_rank_column] = rank_pct.values
            panel.loc[rank_pos.index, future_top_column] = (rank_pos <= future_top_k).astype(float).values

    panel["blended_target"] = build_training_target(panel, config)
    return panel


def build_training_target(panel: pd.DataFrame, config: dict[str, Any]) -> pd.Series:
    """Construct the configured training target for the ranking models."""
    label_cfg = config["labels"]
    target_mode = label_cfg["target_mode"]
    if target_mode == "blended_rank_pct":
        weights = {int(k): float(v) for k, v in label_cfg["blended_rank_weights"].items()}
        target = pd.Series(0.0, index=panel.index, dtype=float)
        total_weight = 0.0
        for horizon, weight in weights.items():
            column = f"future_rank_pct_{horizon}"
            if column in panel.columns:
                target = target.add(panel[column].fillna(0.0) * weight, fill_value=0.0)
                total_weight += weight
        if total_weight <= 0.0:
            return pd.Series(np.nan, index=panel.index, dtype=float)
        valid_mask = pd.Series(False, index=panel.index)
        for horizon in weights:
            column = f"future_rank_pct_{horizon}"
            if column in panel.columns:
                valid_mask = valid_mask | panel[column].notna()
        return (target / total_weight).where(valid_mask)
    if target_mode.startswith("future_rank_pct_"):
        return panel[target_mode]
    if target_mode.startswith("future_return_"):
        return panel[target_mode]
    raise ValueError(f"Unsupported target_mode: {target_mode}")

from __future__ import annotations

from typing import Any

import pandas as pd

from .utils import rank_pct


def compute_rule_scores(panel: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    """Compute configurable rule-based ranking schemes from cross-sectional ranks."""
    panel = panel.copy()
    schemes = config["ranking_schemes"]
    active_scheme = config["rules"]["active_scheme"]
    needed_features = sorted({feature for scheme in schemes.values() for feature in scheme.keys()})

    normalized_features: dict[str, pd.Series] = {}
    for feature in needed_features:
        if feature not in panel.columns:
            raise KeyError(f"Feature '{feature}' referenced by a ranking scheme is missing from the panel.")
        series = panel[feature].where(panel["in_universe"])
        normalized_features[feature] = series.groupby(level="date", group_keys=False).transform(rank_pct)

    for scheme_name, weights in schemes.items():
        score = pd.Series(0.0, index=panel.index)
        for feature_name, weight in weights.items():
            score = score.add(normalized_features[feature_name].fillna(0.0) * float(weight), fill_value=0.0)
        panel[f"rule_score_{scheme_name}"] = score.where(panel["in_universe"])

    panel["rule_score"] = panel[f"rule_score_{active_scheme}"]
    return panel


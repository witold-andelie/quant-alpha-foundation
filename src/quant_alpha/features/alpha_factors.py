from __future__ import annotations

import numpy as np
import pandas as pd

from quant_alpha.config import ProjectConfig
from quant_alpha.features.registry import AlphaDefinition, make_equity_alpha_registry


BASE_FACTOR_COLUMNS = [alpha.name for alpha in make_equity_alpha_registry()]


def _rolling_zscore(series: pd.Series, window: int) -> pd.Series:
    mean = series.rolling(window).mean()
    std = series.rolling(window).std()
    return (series - mean) / std.replace(0, np.nan)


def _breakout_position(series: pd.Series, window: int) -> pd.Series:
    rolling_min = series.rolling(window).min()
    rolling_max = series.rolling(window).max()
    width = (rolling_max - rolling_min).replace(0, np.nan)
    return ((series - rolling_min) / width) - 0.5


def add_alpha_factors(prices: pd.DataFrame, cfg: ProjectConfig) -> pd.DataFrame:
    df = prices.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)
    df["adj_close"] = df["adj_close"].fillna(df["close"])

    forward_days = cfg.backtest.forward_return_days
    grouped = df.groupby("symbol", group_keys=False)

    df["ret_1d"] = grouped["adj_close"].transform(lambda s: s.pct_change())
    df["forward_return"] = grouped["adj_close"].transform(
        lambda s: (s.shift(-forward_days) / s) - 1
    )

    indexed = df.set_index(["date", "symbol"])
    registry = make_equity_alpha_registry()
    for alpha in registry:
        df[alpha.name] = alpha.compute(indexed).reindex(indexed.index).to_numpy()

    ranked_cols = []
    for alpha in registry:
        col = alpha.name
        rank_col = f"{col}_rank"
        df[rank_col] = df.groupby("date")[col].rank(pct=True)
        ranked_cols.append(rank_col)

    df["alpha_composite"] = df[ranked_cols].mean(axis=1) - 0.5
    return df


def alpha_registry_frame(registry: list[AlphaDefinition] | None = None) -> pd.DataFrame:
    registry = registry or make_equity_alpha_registry()
    return pd.DataFrame(
        [
            {
                "alpha_name": alpha.name,
                "expression": alpha.expression,
                "family": alpha.family,
                "hypothesis": alpha.hypothesis,
                "expected_direction": alpha.expected_direction,
            }
            for alpha in registry
        ]
    )

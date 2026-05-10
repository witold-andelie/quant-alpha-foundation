from __future__ import annotations

import numpy as np
import pandas as pd

from quant_alpha.backtest.long_short import run_long_short_backtest
from quant_alpha.config import BacktestConfig


def split_is_oos(panel: pd.DataFrame, split_date: str | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    dates = pd.to_datetime(panel["date"])
    ordered_dates = pd.Series(sorted(dates.dropna().unique()))
    if ordered_dates.empty:
        return panel.iloc[0:0].copy(), panel.iloc[0:0].copy()

    split = pd.Timestamp(split_date) if split_date else ordered_dates.iloc[int(len(ordered_dates) * 0.7)]
    is_panel = panel[dates <= split].copy()
    oos_panel = panel[dates > split].copy()
    return is_panel, oos_panel


def daily_rank_ic(panel: pd.DataFrame, alpha_col: str) -> pd.Series:
    def corr(day: pd.DataFrame) -> float:
        clean = day[[alpha_col, "forward_return"]].dropna()
        if len(clean) < 3:
            return np.nan
        return clean[alpha_col].rank().corr(clean["forward_return"].rank())

    return panel.groupby("date").apply(corr, include_groups=False).dropna()


def _ic_metrics(panel: pd.DataFrame, alpha_col: str, prefix: str) -> dict[str, float]:
    ic = daily_rank_ic(panel, alpha_col)
    if ic.empty:
        return {
            f"{prefix}_ic_mean": np.nan,
            f"{prefix}_ic_std": np.nan,
            f"{prefix}_ic_ir": np.nan,
            f"{prefix}_ic_positive_rate": np.nan,
        }
    std = ic.std(ddof=0)
    return {
        f"{prefix}_ic_mean": float(ic.mean()),
        f"{prefix}_ic_std": float(std),
        f"{prefix}_ic_ir": float(ic.mean() / std) if std > 0 else 0.0,
        f"{prefix}_ic_positive_rate": float((ic > 0).mean()),
    }


def _backtest_metrics(panel: pd.DataFrame, cfg: BacktestConfig, alpha_col: str, prefix: str) -> dict[str, float]:
    _, metrics = run_long_short_backtest(panel, cfg, alpha_col=alpha_col)
    return {f"{prefix}_{key}": value for key, value in metrics.items()}


def alpha_turnover(panel: pd.DataFrame, alpha_cols: list[str], cfg: BacktestConfig) -> pd.DataFrame:
    """Per-factor average daily turnover (fraction of portfolio repositioned)."""
    rows: list[dict[str, object]] = []
    for col in alpha_cols:
        sub = panel.dropna(subset=[col]).copy()
        top_cut = sub.groupby("date")[col].transform(lambda s: s.quantile(1 - cfg.top_quantile))
        bot_cut = sub.groupby("date")[col].transform(lambda s: s.quantile(cfg.bottom_quantile))
        sub["pos"] = 0.0
        sub.loc[sub[col] >= top_cut, "pos"] = 1.0
        sub.loc[sub[col] <= bot_cut, "pos"] = -1.0
        wide = sub.pivot_table(index="date", columns="symbol", values="pos", fill_value=0.0)
        daily_turnover = wide.diff().abs().sum(axis=1)
        rows.append(
            {
                "alpha_name": col,
                "mean_daily_turnover": float(daily_turnover.mean()),
                "median_daily_turnover": float(daily_turnover.median()),
            }
        )
    return pd.DataFrame(rows)


def evaluate_alpha_suite(
    panel: pd.DataFrame,
    alpha_cols: list[str],
    cfg: BacktestConfig,
    split_date: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    is_panel, oos_panel = split_is_oos(panel, split_date)

    diagnostics: list[dict[str, float | str | bool]] = []
    metrics_rows: list[dict[str, float | str]] = []
    backtest_frames: list[pd.DataFrame] = []

    for alpha_col in alpha_cols:
        full_daily, full_metrics = run_long_short_backtest(panel, cfg, alpha_col=alpha_col)
        if not full_daily.empty:
            backtest_frames.append(full_daily.assign(alpha_name=alpha_col))

        row: dict[str, float | str | bool] = {"alpha_name": alpha_col}
        row.update(_ic_metrics(is_panel, alpha_col, "is"))
        row.update(_ic_metrics(oos_panel, alpha_col, "oos"))
        row.update(_backtest_metrics(is_panel, cfg, alpha_col, "is"))
        row.update(_backtest_metrics(oos_panel, cfg, alpha_col, "oos"))
        is_ic = row.get("is_ic_mean", np.nan)
        oos_ic = row.get("oos_ic_mean", np.nan)
        if np.isnan(float(is_ic)) or np.isnan(float(oos_ic)):
            row["is_oos_ic_same_sign"] = None
        else:
            row["is_oos_ic_same_sign"] = bool(np.sign(is_ic) == np.sign(oos_ic))
        row["consistency_score"] = _consistency_score(row)
        row["robustness_score"] = _robustness_score(row)
        diagnostics.append(row)

        metrics_rows.append({"alpha_name": alpha_col, **full_metrics})

    backtests = pd.concat(backtest_frames, ignore_index=True) if backtest_frames else pd.DataFrame()
    return pd.DataFrame(diagnostics), pd.DataFrame(metrics_rows), backtests


def alpha_correlation(panel: pd.DataFrame, alpha_cols: list[str]) -> pd.DataFrame:
    clean = panel[["date", "symbol", *alpha_cols]].dropna(subset=alpha_cols, how="all")
    rows: list[dict[str, float | str]] = []
    for left in alpha_cols:
        for right in alpha_cols:
            corr = clean[left].rank().corr(clean[right].rank())
            rows.append({"alpha_left": left, "alpha_right": right, "spearman_corr": float(corr)})
    return pd.DataFrame(rows)


def select_consistent_alphas(diagnostics: pd.DataFrame, alpha_cols: list[str]) -> list[str]:
    diag = diagnostics.set_index("alpha_name")
    usable = [
        col
        for col in alpha_cols
        if col in diag.index and diag.loc[col, "consistency_score"] >= 0.5
    ]
    if not usable:
        import warnings
        warnings.warn(
            "No alphas passed the consistency threshold; falling back to first 2 by list order.",
            stacklevel=2,
        )
        usable = alpha_cols[:2]
    return usable


def build_orthogonal_composite(panel: pd.DataFrame, diagnostics: pd.DataFrame, alpha_cols: list[str]) -> pd.Series:
    usable = select_consistent_alphas(diagnostics, alpha_cols)

    ranks = [panel.groupby("date")[col].rank(pct=True) - 0.5 for col in usable]
    return pd.concat(ranks, axis=1).mean(axis=1)


def value_added_report(
    panel: pd.DataFrame,
    diagnostics: pd.DataFrame,
    cfg: BacktestConfig,
    composite_col: str = "alpha_composite_research",
) -> pd.DataFrame:
    scored = panel.copy()
    alpha_cols = diagnostics["alpha_name"].tolist()
    selected = select_consistent_alphas(diagnostics, alpha_cols)
    scored[composite_col] = build_orthogonal_composite(scored, diagnostics, alpha_cols)
    _, oos_scored = split_is_oos(scored)
    _, composite_metrics = run_long_short_backtest(oos_scored, cfg, alpha_col=composite_col)
    best_single = diagnostics["oos_sharpe"].max() if "oos_sharpe" in diagnostics else np.nan
    composite_sharpe = composite_metrics.get("sharpe", np.nan)
    return pd.DataFrame(
        [
            {
                "portfolio": composite_col,
                "component_count": len(selected),
                "components": ",".join(selected),
                "best_single_oos_sharpe": best_single,
                "composite_oos_sharpe": composite_sharpe,
                "sharpe_value_added": composite_sharpe - best_single,
                **composite_metrics,
            }
        ]
    )


def _consistency_score(row: dict[str, float | str | bool]) -> float:
    is_ic = float(row.get("is_ic_mean", np.nan))
    oos_ic = float(row.get("oos_ic_mean", np.nan))
    if np.isnan(is_ic) or np.isnan(oos_ic):
        return 0.0
    sign = 1.0 if np.sign(is_ic) == np.sign(oos_ic) else 0.0
    magnitude = min(abs(oos_ic) / max(abs(is_ic), 1e-9), 1.0)
    return float(0.6 * sign + 0.4 * magnitude)


def _robustness_score(row: dict[str, float | str | bool]) -> float:
    oos_obs = float(row.get("oos_observations", 0.0) or 0.0)
    oos_ir = abs(float(row.get("oos_ic_ir", 0.0) or 0.0))
    oos_dd = abs(float(row.get("oos_max_drawdown", 0.0) or 0.0))
    obs_score = min(oos_obs / 252, 1.0)
    ir_score = min(oos_ir / 0.1, 1.0)
    drawdown_score = max(0.0, 1.0 - min(oos_dd, 1.0))
    return float(0.4 * obs_score + 0.4 * ir_score + 0.2 * drawdown_score)

from __future__ import annotations

from pathlib import Path

import pandas as pd

from quant_alpha.backtest.alpha_decay import compute_alpha_decay, walk_forward_ic
from quant_alpha.backtest.diagnostics import alpha_correlation, alpha_turnover, evaluate_alpha_suite, value_added_report
from quant_alpha.backtest.long_short import run_long_short_backtest
from quant_alpha.config import ensure_project_dirs, load_project_config, load_universe
from quant_alpha.features.alpha_factors import BASE_FACTOR_COLUMNS, add_alpha_factors, alpha_registry_frame
from quant_alpha.ingestion.yahoo import fetch_prices
from quant_alpha.storage.duckdb import write_metrics, write_table


def _write_parquet(frame: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)
    return path


def run_pipeline(config_path: Path, root: Path, offline: bool = False) -> dict[str, object]:
    cfg = load_project_config(config_path, root=root)
    ensure_project_dirs(cfg)
    universe = load_universe(cfg.universe_path)

    prices = fetch_prices(cfg, universe, offline=offline)
    prices_path = _write_parquet(prices, cfg.raw_dir / "prices.parquet")
    write_table(cfg.duckdb_path, "raw_prices", prices)

    factors = add_alpha_factors(prices, cfg)
    factors_path = _write_parquet(factors, cfg.processed_dir / "factor_panel.parquet")
    write_table(cfg.duckdb_path, "factor_panel", factors)
    write_table(cfg.duckdb_path, "alpha_registry", alpha_registry_frame())

    backtest_daily, metrics = run_long_short_backtest(factors, cfg.backtest)
    backtest_path = _write_parquet(backtest_daily, cfg.processed_dir / "backtest_daily.parquet")
    write_table(cfg.duckdb_path, "backtest_daily", backtest_daily)
    write_metrics(cfg.duckdb_path, metrics)

    diagnostics, alpha_metrics, alpha_backtests = evaluate_alpha_suite(
        factors,
        BASE_FACTOR_COLUMNS,
        cfg.backtest,
    )
    corr = alpha_correlation(factors, BASE_FACTOR_COLUMNS)
    value_added = value_added_report(factors, diagnostics, cfg.backtest)
    _write_parquet(diagnostics, cfg.processed_dir / "alpha_diagnostics.parquet")
    _write_parquet(alpha_backtests, cfg.processed_dir / "alpha_backtest_daily.parquet")
    write_table(cfg.duckdb_path, "alpha_diagnostics", diagnostics)
    write_table(cfg.duckdb_path, "alpha_metrics", alpha_metrics)
    write_table(cfg.duckdb_path, "alpha_backtest_daily", alpha_backtests)
    write_table(cfg.duckdb_path, "alpha_correlations", corr)
    write_table(cfg.duckdb_path, "alpha_value_added", value_added)

    decay = compute_alpha_decay(factors, BASE_FACTOR_COLUMNS)
    write_table(cfg.duckdb_path, "alpha_decay", decay)

    turnover = alpha_turnover(factors, BASE_FACTOR_COLUMNS, cfg.backtest)
    write_table(cfg.duckdb_path, "alpha_turnover", turnover)

    wf_rows: list[pd.DataFrame] = []
    for col in BASE_FACTOR_COLUMNS:
        wf = walk_forward_ic(factors, col, cfg.backtest)
        if not wf.empty:
            wf_rows.append(wf.assign(alpha_name=col))
    walk_forward = pd.concat(wf_rows, ignore_index=True) if wf_rows else pd.DataFrame()
    write_table(cfg.duckdb_path, "alpha_walk_forward", walk_forward)

    return {
        "prices_path": str(prices_path),
        "factors_path": str(factors_path),
        "backtest_path": str(backtest_path),
        "duckdb_path": str(cfg.duckdb_path),
        "rows": {
            "prices": len(prices),
            "factors": len(factors),
            "backtest_daily": len(backtest_daily),
            "alpha_diagnostics": len(diagnostics),
        },
        "metrics": metrics,
    }

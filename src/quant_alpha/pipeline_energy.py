from __future__ import annotations

from pathlib import Path

import pandas as pd

from quant_alpha.backtest.alpha_decay import compute_energy_alpha_decay
from quant_alpha.backtest.diagnostics import alpha_turnover, evaluate_alpha_suite, value_added_report
from quant_alpha.backtest.long_short import run_long_short_backtest
from quant_alpha.config import ensure_project_dirs, load_project_config, load_yaml
from quant_alpha.features.energy_alpha import ENERGY_ALPHA_EXPRESSIONS, add_energy_alpha_features, energy_alpha_registry_frame
from quant_alpha.ingestion.entsoe import EntsoeClient, fetch_entsoe_power_market
from quant_alpha.ingestion.energy import generate_synthetic_power_market
from quant_alpha.platform.quality import run_energy_quality_checks
from quant_alpha.storage.duckdb import write_metrics, write_table
from quant_alpha.storage.gcp import export_frames_to_gcs_bigquery


def _write_parquet(frame: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)
    return path


def _load_power_market(cfg, markets: list[str], universe: dict[str, object]) -> pd.DataFrame:
    source = cfg.data_source.lower()
    if source == "synthetic":
        return generate_synthetic_power_market(
            markets,
            cfg.start_date,
            cfg.end_date or cfg.start_date,
            freq=cfg.bar_interval,
        )
    if source == "entsoe":
        client = EntsoeClient.from_env(
            token_env=cfg.entsoe.token_env,
            base_url=cfg.entsoe.base_url,
            timeout_seconds=cfg.entsoe.timeout_seconds,
        )
        domains = universe.get("entsoe_domains", {})
        if not isinstance(domains, dict):
            raise ValueError("energy universe entsoe_domains must be a mapping of market to EIC code.")
        return fetch_entsoe_power_market(
            markets=markets,
            domains={str(k): str(v) for k, v in domains.items()},
            start=cfg.start_date,
            end=cfg.end_date or cfg.start_date,
            bar_interval=cfg.bar_interval,
            client=client,
        )
    raise ValueError(f"Unsupported energy data_source: {cfg.data_source}")


def run_energy_pipeline(
    config_path: Path,
    root: Path,
    source_override: str | None = None,
) -> dict[str, object]:
    cfg = load_project_config(config_path, root=root)
    if source_override:
        cfg.data_source = source_override
    ensure_project_dirs(cfg)
    universe = load_yaml(cfg.universe_path)
    markets = universe.get("markets", ["DE_LU", "CZ", "FR"])

    raw = _load_power_market(cfg, markets, universe)
    raw_path = _write_parquet(raw, cfg.raw_dir / "power_market.parquet")
    write_table(cfg.duckdb_path, "power_market_raw", raw)

    quality = run_energy_quality_checks(raw)
    write_table(cfg.duckdb_path, "power_market_quality", quality)

    features = add_energy_alpha_features(raw)
    next_spot = features.groupby("market")["spot_price"].shift(-1)
    # Power markets can have negative/near-zero prices; stabilize denominator for returns.
    denominator = features["spot_price"].abs().clip(lower=20.0)
    features["forward_return"] = ((next_spot - features["spot_price"]) / denominator).clip(-0.8, 0.8)
    features["ret_1d"] = features.groupby("market")["spot_price"].pct_change()
    alpha_cols = list(ENERGY_ALPHA_EXPRESSIONS.keys())
    for col in alpha_cols:
        features[f"{col}_rank"] = features.groupby("timestamp")[col].rank(pct=True)
    features["alpha_composite"] = features[[f"{col}_rank" for col in alpha_cols]].mean(axis=1) - 0.5

    features_path = _write_parquet(features, cfg.processed_dir / "power_market_features.parquet")
    write_table(cfg.duckdb_path, "power_market_features", features)
    write_table(cfg.duckdb_path, "energy_alpha_registry", energy_alpha_registry_frame())

    backtest_panel = features.rename(columns={"timestamp": "date", "market": "symbol"}).copy()

    daily, metrics = run_long_short_backtest(backtest_panel, cfg.backtest, alpha_col="alpha_composite")
    diagnostics, alpha_metrics, alpha_backtests = evaluate_alpha_suite(backtest_panel, alpha_cols, cfg.backtest)
    value_added = value_added_report(backtest_panel, diagnostics, cfg.backtest)

    backtest_path = _write_parquet(daily, cfg.processed_dir / "power_market_backtest.parquet")
    write_table(cfg.duckdb_path, "energy_backtest_daily", daily)
    write_table(cfg.duckdb_path, "energy_alpha_diagnostics", diagnostics)
    write_table(cfg.duckdb_path, "energy_alpha_metrics", alpha_metrics)
    write_table(cfg.duckdb_path, "energy_alpha_backtest_daily", alpha_backtests)
    write_table(cfg.duckdb_path, "energy_alpha_value_added", value_added)
    write_metrics(cfg.duckdb_path, metrics, table_name="energy_backtest_metrics")

    energy_decay = compute_energy_alpha_decay(features, alpha_cols)
    write_table(cfg.duckdb_path, "energy_alpha_decay", energy_decay)

    energy_turnover_panel = backtest_panel.copy()
    energy_turnover_panel["date"] = energy_turnover_panel["date"].astype(str)
    energy_turnover = alpha_turnover(energy_turnover_panel, alpha_cols, cfg.backtest)
    write_table(cfg.duckdb_path, "energy_alpha_turnover", energy_turnover)

    cloud_tables = {
        "power_market_raw": raw,
        "power_market_quality": quality,
        "power_market_features": features,
        "energy_alpha_registry": energy_alpha_registry_frame(),
        "energy_backtest_daily": daily,
        "energy_backtest_metrics": pd.DataFrame([metrics]),
        "energy_alpha_diagnostics": diagnostics,
        "energy_alpha_metrics": alpha_metrics,
        "energy_alpha_backtest_daily": alpha_backtests,
        "energy_alpha_value_added": value_added,
        "energy_alpha_decay": energy_decay,
        "energy_alpha_turnover": energy_turnover,
    }
    cloud_exports = export_frames_to_gcs_bigquery(cloud_tables, cfg.cloud)

    return {
        "raw_path": str(raw_path),
        "features_path": str(features_path),
        "backtest_path": str(backtest_path),
        "duckdb_path": str(cfg.duckdb_path),
        "data_source": cfg.data_source,
        "cloud_exports": cloud_exports,
        "rows": {
            "power_market_raw": len(raw),
            "power_market_features": len(features),
            "energy_alpha_diagnostics": len(diagnostics),
        },
        "metrics": metrics,
    }

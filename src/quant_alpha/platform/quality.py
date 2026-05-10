from __future__ import annotations

import pandas as pd


def validate_primary_key(frame: pd.DataFrame, keys: list[str]) -> dict[str, object]:
    duplicates = frame.duplicated(keys).sum()
    return {"check": "primary_key", "keys": ",".join(keys), "passed": duplicates == 0, "duplicates": int(duplicates)}


def validate_non_null(frame: pd.DataFrame, columns: list[str]) -> list[dict[str, object]]:
    return [
        {
            "check": "non_null",
            "column": col,
            "passed": bool(frame[col].notna().all()),
            "nulls": int(frame[col].isna().sum()),
        }
        for col in columns
    ]


def run_energy_quality_checks(frame: pd.DataFrame) -> pd.DataFrame:
    checks: list[dict[str, object]] = []
    checks.append(validate_primary_key(frame, ["timestamp", "market"]))
    checks.extend(
        validate_non_null(
            frame,
            ["timestamp", "market", "spot_price", "load_forecast", "residual_load"],
        )
    )
    return pd.DataFrame(checks)

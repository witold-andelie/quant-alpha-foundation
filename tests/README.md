# Tests

This directory contains the unit test suite for the platform. Tests verify factor mathematics, pipeline outputs, data quality, and infrastructure configuration. All tests run with `pytest` and require no external services.

## Running Tests

```bash
# All tests
pytest

# With verbose output
pytest -v

# Specific file
pytest tests/test_alpha_factors.py

# Stop on first failure
pytest -x
```

## Test Files

| File | What It Tests |
|---|---|
| `test_alpha_factors.py` | Equity factor computation, panel shape, new factor presence, alpha decay output |
| `test_energy_alpha.py` | Energy factor computation, 8-factor registry, new factor columns, decay shape |
| `test_diagnostics.py` | IS/OOS evaluation, alpha correlation matrix, consistency score range |
| `test_diagnostics.py` | Walk-forward IC structure and value-added report |
| `test_entsoe.py` | ENTSO-E client schema and response normalization |
| `test_quality.py` | Energy data quality check pass/fail logic |
| `test_cloud_export.py` | GCP export function behavior with disabled config |

## Key Assertions

### Factor panel

```python
# All 10 equity factors must appear in the panel
assert len(BASE_FACTOR_COLUMNS) == 10

# Composite and forward return must have non-null values
assert panel["alpha_composite"].notna().sum() > 0
assert panel["forward_return"].notna().sum() > 0
```

### Lookahead guard

```python
# The 21-day momentum factor must be NaN in the first row of each symbol
first_rows = panel.groupby("symbol").head(1)
assert first_rows["alpha_trend_021_medium_momentum"].isna().all()
```

### Alpha decay shape

```python
decay = compute_alpha_decay(panel, test_cols, horizons=[1, 5, 10])
assert len(decay) == len(test_cols) * len(horizons)
assert (decay["ic"].dropna().abs() <= 1.0).all()
```

### Energy registry size

```python
from quant_alpha.features.energy_alpha import ENERGY_ALPHA_REGISTRY
assert len(ENERGY_ALPHA_REGISTRY) == 8
```

## Synthetic Data in Tests

All tests use deterministic synthetic data generators seeded from ticker/market name hashes. This means tests are:

- **Reproducible**: same seed → same output every run
- **Offline**: no network calls required
- **Fast**: typically 1–3 seconds for a full suite run

## CI Integration

Tests run in the first job of the CI pipeline (`lint-and-test`) before any pipeline smoke tests or Docker builds. A failed test blocks all downstream jobs.

```yaml
# .github/workflows/ci.yml
- name: Unit tests
  run: pytest -x -q
```

## Adding a New Test

1. Create or extend a test file in this directory.
2. Import the relevant module and use synthetic data for inputs.
3. Keep each test function focused on a single assertion.
4. Avoid external HTTP calls, file system writes, or DuckDB connections inside unit tests.

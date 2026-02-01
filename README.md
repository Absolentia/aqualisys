# Aqualisys

Polars-first data-quality toolkit delivering deterministic validation, structured logging, and a composable rule registry.

## Why Aqualisys?
- **Declarative rules**: ship reusable expectations such as not-null, uniqueness, accepted-values, and referential checks.
- **Deterministic logging**: every run is persisted to SQLite (JSON-friendly) for audits and debugging.
- **Pipeline-ready**: run from Python code or via `aqualisys validate configs/orders.yml` in CI.

## Quick Start
```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .[dev]
pytest
aqualisys validate configs/orders.yml
```

## Usage Example
```python
import polars as pl
from aqualisys import DataQualityChecker, NotNullRule, UniqueRule, SQLiteRunLogger

df = pl.DataFrame({"order_id": [1, 2, 3], "status": ["pending", "shipped", "shipped"]})
checker = DataQualityChecker(
    rules=[NotNullRule("order_id"), UniqueRule("order_id")],
    logger=SQLiteRunLogger("artifacts/example_runs.db"),
)
report = checker.run(df, dataset_name="orders")
assert report.passed
```

## Project Structure
- `src/aqualisys/`: library source (rules, checker, logging, CLI).
- `tests/`: pytest suites (unit + integration).
- `configs/`: sample validation suite definitions.
- `docs/`: roadmap and design notes.

See `docs/PUBLISHING.md` for uv-based build and release steps once you are ready to publish a new version.

See `docs/ROADMAP.md` for the multi-week implementation plan inspired by the Start Data Engineering guide.

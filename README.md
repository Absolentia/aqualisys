# Aqualisys

Polars-first data-quality toolkit delivering deterministic validation, structured logging, and a composable rule registry.

## Why Aqualisys?
- **Declarative rules**: ship reusable expectations such as not-null, uniqueness, accepted-values, referential checks, and full Polars expression rules.
- **Deterministic logging**: every run is persisted to SQLite (JSON-friendly) for audits and debugging.
- **Pipeline-ready**: run from Python code or via `aqualisys validate configs/orders.yml` in CI.

## Quick Start
```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .[dev]
pre-commit install
pytest
aqualisys validate configs/orders.yml
```

## Usage Example
```python
import polars as pl
from aqualisys import (
    DataQualityChecker,
    ExpressionRule,
    NotNullRule,
    UniqueRule,
    SQLiteRunLogger,
)

df = pl.DataFrame(
    {
        "order_id": [1, 2, 3],
        "status": ["pending", "shipped", "shipped"],
        "total": [10, 20, 10],
    }
)
checker = DataQualityChecker(
    rules=[
        NotNullRule("order_id"),
        UniqueRule("order_id"),
        ExpressionRule("pl.col('total') >= 0", description="Totals stay positive"),
    ],
    logger=SQLiteRunLogger("artifacts/example_runs.db"),
)
report = checker.run(df, dataset_name="orders")
assert report.passed
```

## Rule Catalog

Rules are registered via metadata so configs can reference them by type and even override severity:

```yaml
rules:
  - type: not_null
    column: order_id
  - type: accepted_values
    column: order_status
    allowed_values: ["pending", "shipped", "delivered", "cancelled"]
  - type: expression
    expression: "pl.col('total') >= 0"
    severity: warn
    description: "Order totals must be non-negative"
```

Available built-in types today: `not_null`, `unique`, `accepted_values`, `relationship`, and `expression`. Use `severity: warn|error` per rule and add descriptions for richer logging.

### CLI selectors

The CLI mirrors these selectors:

```bash
aqualisys validate configs/orders.yml \
  --include-tag integrity \
  --exclude-tag experimental \
  --override-severity NotNullRule::order_id=warn
```

Selectors stack with what's defined in the YAML, and `--fail-fast/--no-fail-fast` overrides the config flag at runtime.

## Project Structure
- `src/aqualisys/`: library source (rules, checker, logging, CLI).
- `tests/`: pytest suites (unit + integration).
- `configs/`: sample validation suite definitions.
- `docs/`: roadmap and design notes.

See `docs/PUBLISHING.md` for uv-based build and release steps once you are ready to publish a new version.

See `docs/ROADMAP.md` for the multi-week implementation plan inspired by the Start Data Engineering guide.

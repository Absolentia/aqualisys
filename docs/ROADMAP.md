# Aqualisys Roadmap

## Vision & Scope
- **Audience**: Data engineers who need deterministic, scriptable data-quality checks on Polars DataFrames up to ~100 GB in-memory workloads.
- **Problem**: Automate repetitive assertions (uniqueness, non-null, accepted-value, relationship checks) while logging all outcomes for auditability.
- **Constraints**: Operate on Polars inputs only in v0.x, log validation metadata (dataset, run_id, rule_id, metric/value) to SQLite for easy inspection, keep validator and logger orthogonal so either can be swapped later.

## System Architecture Snapshot
- **Validator Layer**: `DataQualityChecker` orchestrates rule execution. Rules are composable callables implementing a `BaseRule` protocol. Supports registry + bundles for domain suites.
- **Logging Layer**: `SQLiteRunLogger` implements an abstract `RunLogger` interface capturing run context + rule results. Future connectors (DuckDB, HTTP) can reuse the interface.
- **Configuration Layer**: Simple `ValidationConfig` dataclass (YAML/JSON loading later) ties inputs, selected rule bundles, and thresholds. Flags (fail fast, severity) stored here.
- **CLI/Script Layer**: `aqualisys validate --config configs/orders.yml` entry point enabling pipeline integration. CLI delegates entirely to the library.

## Milestones
1. **Foundation (Week 1)**  
   - Scaffold repo: `pyproject.toml`, `src/aqualisys`, `tests`.  
   - Implement minimal Polars rule set (unique, not_null) and SQLite logger.  
   - Provide quick-start documentation + architecture diagrams in README.
2. **Rule Expansion (Week 2)**  
   - Add accepted-values, referential-integrity, expression-based checks.  
   - Introduce rule registry + tagging for bundles.  
   - Emit structured results (JSON + SQLite) to support downstream observability.
3. **Configuration & CLI (Week 3)**  
   - YAML config parser, CLI wrappers for running suites locally or in CI.  
   - Support `--fail-fast`, severity overrides, include/exclude selectors.  
   - Harden logging with retries + summary tables.
4. **DX & Publishing (Week 4)**  
   - Add docs site snippets, end-to-end demo notebook, telemetry opt-in.  
   - Set up `uv build`, publish to TestPyPI, smoke-test install, then promote to PyPI.  
   - Configure CI (lint, type-check, pytest with coverage) and badges.

## Success Metrics
- Unit + integration coverage ≥90% on validators/loggers.  
- Ability to run ≥50 rules/minute on 1M-row datasets on a laptop.  
- Users can onboard by running a single CLI command using sample configs.  
- Release cadence: tagged versions every 2 weeks during v0 development.

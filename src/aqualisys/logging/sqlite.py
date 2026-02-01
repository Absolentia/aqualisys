import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from ..checks.base import RuleContext, RuleResult
from .base import RunLogger


class SQLiteRunLogger(RunLogger):
    """Persists run + rule records to a lightweight SQLite database."""

    def __init__(self, db_path: str | Path = "aqualisys_runs.db") -> None:
        self.db_path = Path(db_path)
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS runs
                (
                    run_id       TEXT PRIMARY KEY,
                    dataset_name TEXT NOT NULL,
                    started_at   TEXT NOT NULL,
                    finished_at  TEXT,
                    total_rules  INTEGER DEFAULT 0,
                    failed_rules INTEGER DEFAULT 0
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS rule_results
                (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id      TEXT NOT NULL,
                    rule_name   TEXT NOT NULL,
                    status      TEXT NOT NULL,
                    severity    TEXT NOT NULL,
                    message     TEXT NOT NULL,
                    metrics     TEXT,
                    recorded_at TEXT NOT NULL,
                    FOREIGN KEY (run_id) REFERENCES runs (run_id)
                )
                """
            )
            conn.commit()

    def log_run_started(self, context: RuleContext) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO runs(run_id, dataset_name, started_at)
                VALUES (?, ?, ?)
                """,
                (context.run_id, context.dataset_name, context.executed_at.isoformat()),
            )
            conn.commit()

    def log_rule_result(self, context: RuleContext, result: RuleResult) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO rule_results(run_id, rule_name, status, severity, message, metrics, recorded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    context.run_id,
                    result.rule_name,
                    result.status.value,
                    result.severity.value,
                    result.message,
                    json.dumps(result.metrics or {}, default=str),
                    datetime.now(tz=timezone.utc).isoformat(),
                ),
            )
            conn.commit()

    def log_run_completed(self, context: RuleContext, results: Iterable[RuleResult]) -> None:
        results_list = list(results)
        failed = sum(1 for result in results_list if not result.passed)
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE runs
                SET finished_at  = ?,
                    total_rules  = ?,
                    failed_rules = ?
                WHERE run_id = ?
                """,
                (
                    datetime.now(tz=timezone.utc).isoformat(),
                    len(results_list),
                    failed,
                    context.run_id,
                ),
            )
            conn.commit()

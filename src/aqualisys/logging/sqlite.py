import json
import sqlite3
import time
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from pathlib import Path

from ..checks.base import RuleContext, RuleResult
from .base import RunLogger


class SQLiteRunLogger(RunLogger):
    """Persists run + rule records to a lightweight SQLite database."""

    def __init__(
        self,
        db_path: str | Path = "aqualisys_runs.db",
        *,
        retries: int = 2,
        retry_delay: float = 0.1,
    ) -> None:
        self.db_path = Path(db_path)
        self._retries = retries
        self._retry_delay = retry_delay
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn

    def _ensure_schema(self) -> None:
        def action(conn: sqlite3.Connection) -> None:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS runs
                (
                    run_id       TEXT PRIMARY KEY,
                    dataset_name TEXT NOT NULL,
                    started_at   TEXT NOT NULL,
                    finished_at  TEXT,
                    total_rules  INTEGER DEFAULT 0,
                    failed_rules INTEGER DEFAULT 0
                )
                """)
            conn.execute("""
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
                """)

        self._execute_with_retry(action)

    def log_run_started(self, context: RuleContext) -> None:
        def action(conn: sqlite3.Connection) -> None:
            conn.execute(
                """
                INSERT OR REPLACE INTO runs(run_id, dataset_name, started_at)
                VALUES (?, ?, ?)
                """,
                (context.run_id, context.dataset_name, context.executed_at.isoformat()),
            )

        self._execute_with_retry(action)

    def log_rule_result(self, context: RuleContext, result: RuleResult) -> None:
        def action(conn: sqlite3.Connection) -> None:
            conn.execute(
                """
                INSERT INTO rule_results(
                    run_id,
                    rule_name,
                    status,
                    severity,
                    message,
                    metrics,
                    recorded_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    context.run_id,
                    result.rule_name,
                    result.status.value,
                    result.severity.value,
                    result.message,
                    json.dumps(result.metrics or {}, default=str),
                    datetime.now(tz=UTC).isoformat(),
                ),
            )

        self._execute_with_retry(action)

    def log_run_completed(
        self,
        context: RuleContext,
        results: Iterable[RuleResult],
    ) -> None:
        results_list = list(results)
        failed = sum(1 for result in results_list if not result.passed)

        def action(conn: sqlite3.Connection) -> None:
            conn.execute(
                """
                UPDATE runs
                SET finished_at  = ?,
                    total_rules  = ?,
                    failed_rules = ?
                WHERE run_id = ?
                """,
                (
                    datetime.now(tz=UTC).isoformat(),
                    len(results_list),
                    failed,
                    context.run_id,
                ),
            )

        self._execute_with_retry(action)

    def _execute_with_retry(self, action: Callable[[sqlite3.Connection], None]) -> None:
        attempt = 0
        while True:
            try:
                with self._connect() as conn:
                    action(conn)
                    conn.commit()
                return
            except sqlite3.OperationalError:
                attempt += 1
                if attempt > self._retries:
                    raise
                time.sleep(self._retry_delay)

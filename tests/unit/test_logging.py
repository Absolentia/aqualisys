import sqlite3

from aqualisys.checks.base import RuleContext, RuleResult, RuleSeverity, RuleStatus
from aqualisys.logging.sqlite import SQLiteRunLogger


def _make_result(name: str = "rule") -> RuleResult:
    return RuleResult(
        rule_name=name,
        status=RuleStatus.PASSED,
        message="ok",
        severity=RuleSeverity.ERROR,
        metrics={},
    )


def test_sqlite_logger_persists_runs(tmp_path):
    db_path = tmp_path / "runs.db"
    logger = SQLiteRunLogger(db_path)
    context = RuleContext(dataset_name="orders", run_id="run-1")
    result = _make_result("UniqueRule::order_id")

    logger.log_run_started(context)
    logger.log_rule_result(context, result)
    logger.log_run_completed(context, [result])

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT dataset_name, total_rules, failed_rules FROM runs WHERE run_id=?",
            (context.run_id,),
        ).fetchone()
        assert row == ("orders", 1, 0)
        rule_row = conn.execute(
            "SELECT rule_name, status FROM rule_results WHERE run_id=?",
            (context.run_id,),
        ).fetchone()
        assert rule_row == (result.rule_name, result.status.value)


def test_sqlite_logger_retries_on_operational_error(monkeypatch, tmp_path):
    db_path = tmp_path / "runs.db"
    logger = SQLiteRunLogger(db_path, retries=1, retry_delay=0)
    context = RuleContext(dataset_name="orders", run_id="run-2")
    attempt = {"count": 0}

    original_connect = logger._connect

    class ProxyConnection:
        def __init__(self, inner):
            self._inner = inner

        def execute(self, sql, params=()):
            if "INSERT OR REPLACE INTO runs" in sql and attempt["count"] == 0:
                attempt["count"] += 1
                raise sqlite3.OperationalError("database is locked")
            return self._inner.execute(sql, params)

        def __getattr__(self, item):
            return getattr(self._inner, item)

        def __enter__(self):
            self._inner.__enter__()
            return self

        def __exit__(self, exc_type, exc, tb):
            return self._inner.__exit__(exc_type, exc, tb)

    def flaky_connect():
        return ProxyConnection(original_connect())

    monkeypatch.setattr(logger, "_connect", flaky_connect)

    logger.log_run_started(context)
    assert attempt["count"] == 1

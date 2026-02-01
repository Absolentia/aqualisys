from dataclasses import dataclass

import pytest

pl = pytest.importorskip("polars")

from aqualisys.checker import DataQualityChecker
from aqualisys.checks.base import BaseRule, RuleResult, RuleSeverity, RuleStatus
from aqualisys.logging.sqlite import SQLiteRunLogger


@dataclass
class DummyRule(BaseRule):
    name: str
    severity: RuleSeverity = RuleSeverity.ERROR
    description: str = "dummy"
    should_fail: bool = False

    def evaluate(self, df: pl.DataFrame) -> RuleResult:
        status = RuleStatus.FAILED if self.should_fail else RuleStatus.PASSED
        return RuleResult(
            rule_name=self.name,
            status=status,
            message="forced result",
            severity=self.severity,
            metrics={"rows": df.height},
        )


def test_checker_with_sqlite_logger(tmp_path):
    df = pl.DataFrame({"col": [1, 2, 3]})
    logger = SQLiteRunLogger(tmp_path / "runs.db")
    checker = DataQualityChecker(
        rules=[DummyRule("rule-pass"), DummyRule("rule-fail", should_fail=True)],
        logger=logger,
        fail_fast=True,
    )

    report = checker.run(df, dataset_name="orders")
    assert not report.passed
    assert len(report.failed_rules) == 1

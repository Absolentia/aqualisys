from pathlib import Path

import pytest

from aqualisys.checks.base import RuleSeverity
from aqualisys.checks.rules import ExpressionRule
from aqualisys.config import ValidationSuiteConfig

pl = pytest.importorskip("polars")


def test_build_rules_uses_registry_expression(tmp_path):
    config = ValidationSuiteConfig(
        dataset_name="orders",
        dataset_path=Path("orders.parquet"),
        rules=[
            {
                "type": "expression",
                "expression": "pl.col('total') > 0",
                "severity": "warn",
            }
        ],
    )
    rules = config.build_rules()
    assert len(rules) == 1
    rule = rules[0]
    assert isinstance(rule, ExpressionRule)
    assert rule.severity is RuleSeverity.WARN

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


def test_include_exclude_tags_filter_rules(tmp_path):
    config = ValidationSuiteConfig(
        dataset_name="orders",
        dataset_path=Path("orders.parquet"),
        include_tags=("expression",),
        rules=[
            {"type": "not_null", "column": "order_id"},
            {"type": "expression", "expression": "pl.col('total') >= 0"},
        ],
    )
    rules = config.build_rules()
    assert len(rules) == 1
    assert isinstance(rules[0], ExpressionRule)


def test_severity_override_applies_by_rule_name(tmp_path):
    config = ValidationSuiteConfig(
        dataset_name="orders",
        dataset_path=Path("orders.parquet"),
        severity_overrides={"NotNullRule::order_id": "warn"},
        rules=[{"type": "not_null", "column": "order_id"}],
    )
    rule = config.build_rules()[0]
    assert rule.severity is RuleSeverity.WARN

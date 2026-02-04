import pytest

from aqualisys.checks.rules import (
    AcceptedValuesRule,
    ExpressionRule,
    NotNullRule,
    RelationshipRule,
    UniqueRule,
)

pl = pytest.importorskip("polars")


def test_not_null_rule_passes():
    df = pl.DataFrame({"col": [1, 2]})
    result = NotNullRule("col").evaluate(df)
    assert result.passed


def test_not_null_rule_fails():
    df = pl.DataFrame({"col": [1, None]})
    result = NotNullRule("col").evaluate(df)
    assert not result.passed
    assert result.metrics["null_count"] == 1


def test_unique_rule_detects_duplicates():
    df = pl.DataFrame({"col": [1, 1, 2]})
    result = UniqueRule("col").evaluate(df)
    assert not result.passed
    assert result.metrics["duplicate_count"] == 1


def test_accepted_values_rule():
    df = pl.DataFrame({"col": ["ok", "bad"]})
    result = AcceptedValuesRule("col", ["ok"]).evaluate(df)
    assert not result.passed
    assert result.metrics["violation_count"] == 1


def test_relationship_rule_respects_reference():
    orders = pl.DataFrame({"customer_id": [1, 2, 3]})
    customers = pl.DataFrame({"id": [1, 2]})
    result = RelationshipRule(
        "customer_id",
        customers.rename({"id": "customer_id"}),
        "customer_id",
    ).evaluate(orders)
    assert not result.passed
    assert result.metrics["violation_count"] == 1


def test_expression_rule_passes_when_expression_holds():
    df = pl.DataFrame({"a": [1, 2], "b": [2, 4]})
    rule = ExpressionRule("pl.col('b') >= pl.col('a')")
    assert rule.evaluate(df).passed


def test_expression_rule_reports_violations():
    df = pl.DataFrame({"a": [1, -2], "b": [0, 0]})
    rule = ExpressionRule("pl.col('a') >= 0")
    result = rule.evaluate(df)
    assert not result.passed
    assert result.metrics["violation_count"] == 1


def test_expression_rule_requires_polars_expression():
    df = pl.DataFrame({"a": [1]})
    rule = ExpressionRule("42")
    with pytest.raises(ValueError):
        rule.evaluate(df)

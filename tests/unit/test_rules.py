import pytest

pl = pytest.importorskip("polars")

from aqualisys.checks.rules import (
    AcceptedValuesRule,
    NotNullRule,
    RelationshipRule,
    UniqueRule,
)


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
    result = RelationshipRule("customer_id", customers.rename({"id": "customer_id"}), "customer_id").evaluate(orders)
    assert not result.passed
    assert result.metrics["violation_count"] == 1

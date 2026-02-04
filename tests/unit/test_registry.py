import pytest

from aqualisys.checks.registry import get_rule, list_rules

pl = pytest.importorskip("polars")


def test_get_rule_returns_definition():
    definition = get_rule("not_null")
    rule = definition.builder({"type": "not_null", "column": "col"})
    assert definition.name == "not_null"
    assert "nulls" in definition.tags
    assert rule.name.startswith("NotNullRule")


def test_list_rules_filters_by_tag():
    rules = list_rules(tag="expression")
    assert len(rules) == 1
    assert rules[0].name == "expression"


def test_get_rule_raises_for_unknown_type():
    with pytest.raises(KeyError):
        get_rule("does_not_exist")

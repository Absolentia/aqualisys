from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl

from .base import BaseRule, RuleSeverity
from .rules import (
    AcceptedValuesRule,
    ExpressionRule,
    NotNullRule,
    RelationshipRule,
    UniqueRule,
)

RuleFactory = Callable[[Mapping[str, Any]], BaseRule]


@dataclass(frozen=True)
class RuleDefinition:
    name: str
    description: str
    tags: frozenset[str]
    builder: RuleFactory


_REGISTRY: dict[str, RuleDefinition] = {}


def _register_builtin(
    name: str,
    builder: RuleFactory,
    *,
    description: str,
    tags: Iterable[str],
) -> None:
    register_rule(name, builder, description=description, tags=tags)


def register_rule(
    name: str,
    builder: RuleFactory,
    *,
    description: str = "",
    tags: Iterable[str] | None = None,
) -> None:
    key = name.lower()
    if key in _REGISTRY:
        raise ValueError(f"rule '{name}' is already registered")
    _REGISTRY[key] = RuleDefinition(
        name=name,
        description=description,
        tags=frozenset(tags or ()),
        builder=builder,
    )


def get_rule(name: str) -> RuleDefinition:
    try:
        return _REGISTRY[name.lower()]
    except KeyError as exc:  # pragma: no cover - defensive
        raise KeyError(f"unknown rule type: {name}") from exc


def list_rules(tag: str | None = None) -> list[RuleDefinition]:
    definitions = _REGISTRY.values()
    if tag:
        tag = tag.lower()
        definitions = [
            definition for definition in definitions if tag in definition.tags
        ]
    return sorted(definitions, key=lambda definition: definition.name)


def _resolve_severity(config: Mapping[str, Any]) -> RuleSeverity:
    level = config.get("severity")
    if not level:
        return RuleSeverity.ERROR
    try:
        return RuleSeverity(level.lower())
    except ValueError as exc:
        raise ValueError(f"unknown severity '{level}'") from exc


def _resolve_description(
    config: Mapping[str, Any],
    fallback: str,
) -> str:
    return config.get("description") or fallback


def _build_not_null(config: Mapping[str, Any]) -> BaseRule:
    return NotNullRule(
        column=config["column"],
        severity=_resolve_severity(config),
        description=_resolve_description(config, f"NotNull on {config['column']}"),
    )


def _build_unique(config: Mapping[str, Any]) -> BaseRule:
    return UniqueRule(
        column=config["column"],
        severity=_resolve_severity(config),
        description=_resolve_description(config, f"Unique on {config['column']}"),
    )


def _build_accepted_values(config: Mapping[str, Any]) -> BaseRule:
    return AcceptedValuesRule(
        column=config["column"],
        allowed_values=config["allowed_values"],
        severity=_resolve_severity(config),
    )


def _build_relationship(config: Mapping[str, Any]) -> BaseRule:
    reference_cfg = config["reference"]
    ref_path = Path(reference_cfg["path"])
    ref_format = reference_cfg.get("format", "parquet")
    if ref_format == "parquet":
        reference_df = pl.read_parquet(ref_path)
    elif ref_format == "csv":
        reference_df = pl.read_csv(ref_path)
    else:  # pragma: no cover - validated via config tests
        raise ValueError(f"unsupported reference format: {ref_format}")
    return RelationshipRule(
        column=config["column"],
        reference_df=reference_df,
        reference_column=reference_cfg["column"],
        severity=_resolve_severity(config),
    )


def _build_expression(config: Mapping[str, Any]) -> BaseRule:
    return ExpressionRule(
        expression=config["expression"],
        severity=_resolve_severity(config),
        description=_resolve_description(
            config,
            f"Expression rule {config['expression']}",
        ),
    )


_register_builtin(
    name="not_null",
    builder=_build_not_null,
    description="Fails when the specified column contains null values.",
    tags=("nulls", "integrity"),
)
_register_builtin(
    name="unique",
    builder=_build_unique,
    description="Fails when duplicate values are detected in the column.",
    tags=("uniqueness", "integrity"),
)
_register_builtin(
    name="accepted_values",
    builder=_build_accepted_values,
    description="Ensures all column values are part of an allowed set.",
    tags=("reference", "categorical"),
)
_register_builtin(
    name="relationship",
    builder=_build_relationship,
    description="Verifies referential integrity with an on-disk reference dataset.",
    tags=("reference", "integrity"),
)
_register_builtin(
    name="expression",
    builder=_build_expression,
    description="Evaluates a boolean Polars expression defined as a string.",
    tags=("expression", "flexible"),
)

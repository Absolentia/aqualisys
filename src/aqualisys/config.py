from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, ClassVar

import yaml

try:
    import polars as pl
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    pl = None  # type: ignore

from .checker import DataQualityChecker
from .checks.base import BaseRule, RuleSeverity
from .checks.registry import get_rule
from .logging.sqlite import SQLiteRunLogger


@dataclass(slots=True)
class ValidationSuiteConfig:
    dataset_name: str
    dataset_path: Path
    dataset_format: str = "parquet"
    fail_fast: bool = False
    rules: list[Mapping[str, Any]] | None = None
    logger_path: Path = Path("aqualisys_runs.db")
    include_tags: tuple[str, ...] = ()
    exclude_tags: tuple[str, ...] = ()
    severity_overrides: Mapping[str, str] = field(default_factory=dict)

    SUPPORTED_FORMATS: ClassVar[set[str]] = {"parquet", "csv"}

    @classmethod
    def from_yaml(cls, path: str | Path) -> ValidationSuiteConfig:
        data = yaml.safe_load(Path(path).read_text())
        selectors = data.get("selectors", {})
        return cls(
            dataset_name=data["dataset"]["name"],
            dataset_path=Path(data["dataset"]["path"]),
            dataset_format=data["dataset"].get("format", "parquet"),
            fail_fast=data.get("fail_fast", False),
            rules=data.get("rules", []),
            logger_path=Path(data.get("logger", {}).get("path", "aqualisys_runs.db")),
            include_tags=tuple(
                tag.lower() for tag in selectors.get("include_tags", [])
            ),
            exclude_tags=tuple(
                tag.lower() for tag in selectors.get("exclude_tags", [])
            ),
            severity_overrides={
                key: value
                for key, value in (data.get("severity_overrides") or {}).items()
            },
        )

    def load_dataframe(self) -> pl.DataFrame:
        if pl is None:  # pragma: no cover - guard for environments lacking polars
            raise RuntimeError("polars is required to load dataframes")
        if self.dataset_format not in self.SUPPORTED_FORMATS:
            raise ValueError(f"unsupported dataset format: {self.dataset_format}")
        if self.dataset_format == "parquet":
            return pl.read_parquet(self.dataset_path)
        return pl.read_csv(self.dataset_path)

    def build_rules(self) -> list[BaseRule]:
        all_rules: list[BaseRule] = []
        for config in self.rules or []:
            rule_type = config.get("type")
            if not rule_type:
                raise ValueError("rule entry missing 'type'")
            try:
                definition = get_rule(rule_type)
            except KeyError as exc:
                raise ValueError(f"unknown rule type: {rule_type}") from exc
            if not self._matches_selectors(definition.tags):
                continue
            rule = definition.builder(config)
            all_rules.append(rule)

        for name, level in self.severity_overrides.items():
            self._apply_severity_override(all_rules, name, level)
        return all_rules

    def with_overrides(
        self,
        *,
        include_tags: tuple[str, ...] = (),
        exclude_tags: tuple[str, ...] = (),
        severity_overrides: Mapping[str, str] | None = None,
        fail_fast: bool | None = None,
    ) -> ValidationSuiteConfig:
        merged = dict(self.severity_overrides)
        if severity_overrides:
            merged.update(severity_overrides)
        return replace(
            self,
            include_tags=self.include_tags + tuple(tag.lower() for tag in include_tags),
            exclude_tags=self.exclude_tags + tuple(tag.lower() for tag in exclude_tags),
            severity_overrides=merged,
            fail_fast=self.fail_fast if fail_fast is None else fail_fast,
        )

    def _matches_selectors(self, tags: frozenset[str]) -> bool:
        lowered = {tag.lower() for tag in tags}
        if self.include_tags and not lowered.intersection(self.include_tags):
            return False
        if self.exclude_tags and lowered.intersection(self.exclude_tags):
            return False
        return True

    def _apply_severity_override(
        self,
        rules: list[BaseRule],
        rule_name: str,
        level: str,
    ) -> None:
        try:
            severity = RuleSeverity(level.lower())
        except ValueError as exc:
            raise ValueError(f"unknown severity override level: {level}") from exc
        for rule in rules:
            if rule.name == rule_name:
                rule.severity = severity  # type: ignore[assignment]
                break

    def build_checker(self) -> DataQualityChecker:
        logger = SQLiteRunLogger(self.logger_path)
        return DataQualityChecker(
            rules=self.build_rules(),
            logger=logger,
            fail_fast=self.fail_fast,
        )

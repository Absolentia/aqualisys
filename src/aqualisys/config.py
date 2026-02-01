from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar

import yaml

try:
    import polars as pl
except ModuleNotFoundError:
    # pragma: no cover - optional dependency
    pl = None  # type: ignore

from .checker import DataQualityChecker
from .checks.base import BaseRule
from .checks.rules import AcceptedValuesRule, NotNullRule, RelationshipRule, UniqueRule
from .logging.sqlite import SQLiteRunLogger

RuleFactory = Callable[[Mapping[str, Any]], BaseRule]


def _build_not_null(config: Mapping[str, Any]) -> BaseRule:
    return NotNullRule(column=config["column"])


def _build_unique(config: Mapping[str, Any]) -> BaseRule:
    return UniqueRule(column=config["column"])


def _build_accepted(config: Mapping[str, Any]) -> BaseRule:
    return AcceptedValuesRule(
        column=config["column"],
        allowed_values=config["allowed_values"],
    )


def _build_relationship(config: Mapping[str, Any]) -> BaseRule:
    if pl is None:
        # pragma: no cover - config is still valid without runtime Polars
        raise RuntimeError("polars is required for relationship rules")
    ref_path = Path(config["reference"]["path"])
    ref_format = config["reference"].get("format", "parquet")
    if ref_format == "parquet":
        reference_df = pl.read_parquet(ref_path)
    elif ref_format == "csv":
        reference_df = pl.read_csv(ref_path)
    else:  # pragma: no cover - validated elsewhere
        raise ValueError(f"unsupported reference format: {ref_format}")
    return RelationshipRule(
        column=config["column"],
        reference_df=reference_df,
        reference_column=config["reference"]["column"],
    )


RULE_BUILDERS: dict[str, RuleFactory] = {
    "not_null": _build_not_null,
    "unique": _build_unique,
    "accepted_values": _build_accepted,
    "relationship": _build_relationship,
}


@dataclass(slots=True)
class ValidationSuiteConfig:
    dataset_name: str
    dataset_path: Path
    dataset_format: str = "parquet"
    fail_fast: bool = False
    rules: list[Mapping[str, Any]] | None = None
    logger_path: Path = Path("aqualisys_runs.db")

    SUPPORTED_FORMATS: ClassVar[set[str]] = {"parquet", "csv"}

    @classmethod
    def from_yaml(cls, path: str | Path) -> "ValidationSuiteConfig":
        data = yaml.safe_load(Path(path).read_text())
        return cls(
            dataset_name=data["dataset"]["name"],
            dataset_path=Path(data["dataset"]["path"]),
            dataset_format=data["dataset"].get("format", "parquet"),
            fail_fast=data.get("fail_fast", False),
            rules=data.get("rules", []),
            logger_path=Path(data.get("logger", {}).get("path", "aqualisys_runs.db")),
        )

    def load_dataframe(self) -> "pl.DataFrame":
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
            rule_type = config["type"]
            builder = RULE_BUILDERS.get(rule_type)
            if not builder:
                raise ValueError(f"unknown rule type: {rule_type}")
            all_rules.append(builder(config))
        return all_rules

    def build_checker(self) -> DataQualityChecker:
        logger = SQLiteRunLogger(self.logger_path)
        return DataQualityChecker(
            rules=self.build_rules(),
            logger=logger,
            fail_fast=self.fail_fast,
        )

from dataclasses import dataclass
from typing import Iterable

import polars as pl

from src.aqualisys.checks.base import BaseRule, RuleResult, RuleSeverity, RuleStatus


@dataclass(slots=True)
class ColumnRule(BaseRule):
    column: str
    severity: RuleSeverity = RuleSeverity.ERROR
    description: str | None = None

    def __post_init__(self) -> None:
        self.description = self.description or f"{self.__class__.__name__} on {self.column}"

    @property
    def name(self) -> str:
        return f"{self.__class__.__name__}::{self.column}"


class NotNullRule(ColumnRule):
    def evaluate(self, df: pl.DataFrame) -> RuleResult:
        nulls = df.select(pl.col(self.column).is_null().sum().alias("nulls")).item()
        status = RuleStatus.PASSED if nulls == 0 else RuleStatus.FAILED
        message = "column has no nulls" if status is RuleStatus.PASSED else f"{nulls} null values found"
        return RuleResult(
            rule_name=self.name,
            status=status,
            message=message,
            severity=self.severity,
            metrics={"null_count": nulls},
        )


class UniqueRule(ColumnRule):
    def evaluate(self, df: pl.DataFrame) -> RuleResult:
        duplicates = (
            df.select(pl.col(self.column).is_duplicated().sum().alias("dupes")).item()
        )
        status = RuleStatus.PASSED if duplicates == 0 else RuleStatus.FAILED
        message = "column values are unique" if status is RuleStatus.PASSED else f"{duplicates} duplicate rows found"
        return RuleResult(
            rule_name=self.name,
            status=status,
            message=message,
            severity=self.severity,
            metrics={"duplicate_count": duplicates},
        )


class AcceptedValuesRule(ColumnRule):
    def __init__(self, column: str, allowed_values: Iterable[str | int | float],
                 severity: RuleSeverity = RuleSeverity.ERROR):
        super().__init__(column=column, severity=severity)
        self.allowed_values = tuple(dict.fromkeys(allowed_values))  # stable + deduped

    def evaluate(self, df: pl.DataFrame) -> RuleResult:
        violations = (
            df.filter(~pl.col(self.column).is_in(self.allowed_values))
            .select(pl.len())
            .item()
        )
        status = RuleStatus.PASSED if violations == 0 else RuleStatus.FAILED
        message = "column values match allowed set" if status is RuleStatus.PASSED else f"{violations} disallowed values detected"
        return RuleResult(
            rule_name=self.name,
            status=status,
            message=message,
            severity=self.severity,
            metrics={"violation_count": violations, "allowed_values": self.allowed_values},
        )


class RelationshipRule(ColumnRule):
    def __init__(
            self,
            column: str,
            reference_df: pl.DataFrame,
            reference_column: str,
            severity: RuleSeverity = RuleSeverity.ERROR,
    ) -> None:
        super().__init__(column=column, severity=severity)
        self._reference_df = reference_df.select(reference_column)
        self._reference_column = reference_column

    def evaluate(self, df: pl.DataFrame) -> RuleResult:
        reference_set = set(self._reference_df[self._reference_column].to_list())
        violations = (
            df.filter(~pl.col(self.column).is_in(reference_set))
            .select(pl.len())
            .item()
        )
        status = RuleStatus.PASSED if violations == 0 else RuleStatus.FAILED
        message = (
            "referential integrity holds"
            if status is RuleStatus.PASSED
            else f"{violations} values missing from reference {self._reference_column}"
        )
        return RuleResult(
            rule_name=self.name,
            status=status,
            message=message,
            severity=self.severity,
            metrics={
                "violation_count": violations,
                "reference_column": self._reference_column,
                "reference_size": len(reference_set),
            },
        )

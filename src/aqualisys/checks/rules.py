from collections.abc import Iterable
from dataclasses import dataclass

import polars as pl

from .base import BaseRule, RuleResult, RuleSeverity, RuleStatus


@dataclass(slots=True)
class ColumnRule(BaseRule):
    column: str
    severity: RuleSeverity = RuleSeverity.ERROR
    description: str | None = None

    def __post_init__(self) -> None:
        self.description = (
            self.description or f"{self.__class__.__name__} on {self.column}"
        )

    @property
    def name(self) -> str:
        return f"{self.__class__.__name__}::{self.column}"


class NotNullRule(ColumnRule):
    def evaluate(self, df: pl.DataFrame) -> RuleResult:
        nulls = df.select(pl.col(self.column).is_null().sum().alias("nulls")).item()
        status = RuleStatus.PASSED if nulls == 0 else RuleStatus.FAILED
        message = (
            "column has no nulls"
            if status is RuleStatus.PASSED
            else f"{nulls} null values found"
        )
        return RuleResult(
            rule_name=self.name,
            status=status,
            message=message,
            severity=self.severity,
            metrics={"null_count": nulls},
        )


class UniqueRule(ColumnRule):
    def evaluate(self, df: pl.DataFrame) -> RuleResult:
        total_rows = df.height
        unique_rows = df.select(pl.col(self.column).n_unique().alias("unique")).item()
        duplicates = total_rows - unique_rows
        status = RuleStatus.PASSED if duplicates == 0 else RuleStatus.FAILED
        message = (
            "column values are unique"
            if status is RuleStatus.PASSED
            else f"{duplicates} duplicate rows found"
        )
        return RuleResult(
            rule_name=self.name,
            status=status,
            message=message,
            severity=self.severity,
            metrics={"duplicate_count": duplicates},
        )


class AcceptedValuesRule(ColumnRule):
    def __init__(
        self,
        column: str,
        allowed_values: Iterable[str | int | float],
        severity: RuleSeverity = RuleSeverity.ERROR,
    ):
        super().__init__(column=column, severity=severity)
        self.allowed_values = tuple(dict.fromkeys(allowed_values))  # stable + deduped

    def evaluate(self, df: pl.DataFrame) -> RuleResult:
        violations = (
            df.filter(~pl.col(self.column).is_in(self.allowed_values))
            .select(pl.len())
            .item()
        )
        status = RuleStatus.PASSED if violations == 0 else RuleStatus.FAILED
        message = (
            "column values match allowed set"
            if status is RuleStatus.PASSED
            else f"{violations} disallowed values detected"
        )
        return RuleResult(
            rule_name=self.name,
            status=status,
            message=message,
            severity=self.severity,
            metrics={
                "violation_count": violations,
                "allowed_values": self.allowed_values,
            },
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
            df.filter(~pl.col(self.column).is_in(reference_set)).select(pl.len()).item()
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


class ExpressionRule(BaseRule):
    """Evaluates a boolean Polars expression string for every row."""

    def __init__(
        self,
        expression: str,
        *,
        severity: RuleSeverity = RuleSeverity.ERROR,
        description: str | None = None,
    ) -> None:
        self.expression = expression
        self.severity = severity
        self.description = description or f"ExpressionRule on {expression}"

    @property
    def name(self) -> str:
        return f"ExpressionRule::{self.expression}"

    def _compile(self) -> pl.Expr:
        try:
            compiled = eval(self.expression, {"pl": pl}, {})
        except Exception as exc:  # pragma: no cover - exercised via tests
            raise ValueError(f"invalid expression: {self.expression}") from exc
        if not isinstance(compiled, pl.Expr):
            raise ValueError(
                "expression must evaluate to a Polars expression, got "
                f"{type(compiled)!r}"
            )
        return compiled

    def evaluate(self, df: pl.DataFrame) -> RuleResult:
        expr = self._compile()
        result_series = df.select(expr.alias("result")).to_series()
        violations = int((~result_series).sum())
        status = RuleStatus.PASSED if violations == 0 else RuleStatus.FAILED
        message = (
            "expression satisfied for all rows"
            if status is RuleStatus.PASSED
            else f"{violations} expression violations detected"
        )
        return RuleResult(
            rule_name=self.name,
            status=status,
            message=message,
            severity=self.severity,
            metrics={
                "expression": self.expression,
                "violation_count": violations,
            },
        )

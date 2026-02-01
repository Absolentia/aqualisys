from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Mapping, Protocol, runtime_checkable

try:
    import polars as pl
except ModuleNotFoundError:  # pragma: no cover - polars is an optional runtime dependency in tests
    pl = None  # type: ignore


class RuleSeverity(str, Enum):
    ERROR = "error"
    WARN = "warn"


class RuleStatus(str, Enum):
    PASSED = "passed"
    FAILED = "failed"


@dataclass(slots=True)
class RuleResult:
    """Represents the outcome of a single rule."""

    rule_name: str
    status: RuleStatus
    message: str
    severity: RuleSeverity
    metrics: Mapping[str, Any] | None = None

    @property
    def passed(self) -> bool:
        return self.status is RuleStatus.PASSED


@dataclass(slots=True)
class RuleContext:
    dataset_name: str
    run_id: str
    executed_at: datetime = datetime.now(tz=timezone.utc)


@runtime_checkable
class BaseRule(Protocol):
    """All validation rules must follow this shape."""

    name: str
    description: str
    severity: RuleSeverity

    def evaluate(self, df: "pl.DataFrame") -> RuleResult:  # pragma: no cover - protocol
        ...

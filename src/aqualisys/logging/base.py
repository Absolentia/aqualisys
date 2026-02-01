from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime

from ..checks.base import RuleContext, RuleResult


@dataclass(slots=True)
class RunSummary:
    run_id: str
    dataset_name: str
    started_at: datetime
    finished_at: datetime = field(default_factory=lambda: datetime.now(tz=UTC))


class RunLogger(ABC):
    """Interface for persisting run metadata + rule outcomes."""

    @abstractmethod
    def log_run_started(self, context: RuleContext) -> None:  # pragma: no cover - interface
        ...

    @abstractmethod
    def log_rule_result(self, context: RuleContext, result: RuleResult) -> None:  # pragma: no cover - interface
        ...

    @abstractmethod
    def log_run_completed(self, context: RuleContext, results: Iterable[RuleResult]) -> None:  # pragma: no cover
        ...

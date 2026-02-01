from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from uuid import uuid4

try:
    import polars as pl
except ModuleNotFoundError:
    # pragma: no cover - optional dependency in some environments
    pl = None  # type: ignore

from .checks.base import BaseRule, RuleContext, RuleResult, RuleSeverity
from .logging.base import RunLogger


@dataclass(slots=True)
class RuleBundle:
    name: str
    description: str
    rule_factory: Callable[[], Sequence[BaseRule]]

    def rules(self) -> list[BaseRule]:
        return list(self.rule_factory())


@dataclass(slots=True)
class ValidationReport:
    run_id: str
    dataset_name: str
    results: list[RuleResult]

    @property
    def passed(self) -> bool:
        return all(result.passed for result in self.results)

    @property
    def failed_rules(self) -> list[RuleResult]:
        return [result for result in self.results if not result.passed]


class DataQualityChecker:
    """Coordinates rule execution and logging."""

    def __init__(
        self,
        rules: Iterable[BaseRule] | None = None,
        bundles: Iterable[RuleBundle] | None = None,
        logger: RunLogger | None = None,
        fail_fast: bool = False,
    ) -> None:
        self._rules: list[BaseRule] = list(rules or [])
        for bundle in bundles or []:
            self._rules.extend(bundle.rules())
        self._logger = logger
        self._fail_fast = fail_fast

    @property
    def rules(self) -> list[BaseRule]:
        return list(self._rules)

    def add_rules(self, *rules: BaseRule) -> None:
        self._rules.extend(rules)

    def run(
        self,
        dataframe: "pl.DataFrame",
        dataset_name: str,
        run_id: str | None = None,
    ) -> ValidationReport:
        if pl is None:  # pragma: no cover - guard for environments without polars
            raise RuntimeError("polars is required to run validations")

        run_id = run_id or str(uuid4())
        context = RuleContext(dataset_name=dataset_name, run_id=run_id)
        results: list[RuleResult] = []

        if self._logger:
            self._logger.log_run_started(context)

        for rule in self._rules:
            result = rule.evaluate(dataframe)
            results.append(result)
            if self._logger:
                self._logger.log_rule_result(context, result)
            if (
                self._fail_fast
                and not result.passed
                and rule.severity is RuleSeverity.ERROR
            ):
                break

        if self._logger:
            self._logger.log_run_completed(context, results)

        return ValidationReport(
            run_id=run_id,
            dataset_name=dataset_name,
            results=results,
        )

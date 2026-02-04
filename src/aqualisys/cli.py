import json
from pathlib import Path

import click

from .config import ValidationSuiteConfig


@click.group()
def cli() -> None:
    """CLI entry point for running data quality suites."""


@cli.command("validate")
@click.argument(
    "config_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
)
@click.option(
    "--include-tag",
    "include_tags",
    multiple=True,
    help="Only run rules that include the specified registry tag (can repeat).",
)
@click.option(
    "--exclude-tag",
    "exclude_tags",
    multiple=True,
    help="Skip rules that include the specified registry tag (can repeat).",
)
@click.option(
    "--override-severity",
    "severity_overrides",
    multiple=True,
    help="Override rule severity using 'rule_name=warn|error'.",
)
@click.option(
    "--fail-fast/--no-fail-fast",
    "fail_fast_override",
    default=None,
    help="Override fail-fast behavior defined in the config.",
)
def validate_command(
    config_path: Path,
    include_tags: tuple[str, ...],
    exclude_tags: tuple[str, ...],
    severity_overrides: tuple[str, ...],
    fail_fast_override: bool | None,
) -> None:
    """Run the configured validation suite and emit a JSON summary."""

    suite = ValidationSuiteConfig.from_yaml(config_path).with_overrides(
        include_tags=include_tags,
        exclude_tags=exclude_tags,
        severity_overrides=_parse_severity_overrides(severity_overrides),
        fail_fast=fail_fast_override,
    )
    dataframe = suite.load_dataframe()
    checker = suite.build_checker()
    report = checker.run(dataframe, dataset_name=suite.dataset_name)

    summary = {
        "run_id": report.run_id,
        "dataset": report.dataset_name,
        "passed": report.passed,
        "failed_rules": [result.rule_name for result in report.failed_rules],
    }
    click.echo(json.dumps(summary, indent=2))

    if not report.passed:
        raise SystemExit(1)


def run() -> None:
    cli(prog_name="aqualisys")


if __name__ == "__main__":  # pragma: no cover - script entry point
    run()


def _parse_severity_overrides(items: tuple[str, ...]) -> dict[str, str]:
    overrides: dict[str, str] = {}
    for item in items:
        if "=" not in item:
            raise click.BadParameter(
                f"Invalid override '{item}'. Expected format 'rule_name=warn|error'."
            )
        rule_name, level = item.split("=", 1)
        overrides[rule_name.strip()] = level.strip()
    return overrides

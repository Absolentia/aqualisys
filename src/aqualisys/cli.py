import json
from pathlib import Path

import click

from .config import ValidationSuiteConfig


@click.group()
def cli() -> None:
    """CLI entry point for running data quality suites."""


@cli.command("validate")
@click.argument("config_path", type=click.Path(exists=True, dir_okay=False, path_type=Path))
def validate_command(config_path: Path) -> None:
    """Run the configured validation suite and emit a JSON summary."""

    suite = ValidationSuiteConfig.from_yaml(config_path)
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

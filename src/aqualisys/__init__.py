"""
Aqualisys: a Polars-first data quality toolkit.

Expose the key classes so downstream users can import from `aqualisys`.
"""

from src.aqualisys.checker import DataQualityChecker, RuleBundle
from src.aqualisys.checks.rules import AcceptedValuesRule, NotNullRule, RelationshipRule, UniqueRule
from src.aqualisys.logging.sqlite import SQLiteRunLogger

__all__ = [
    "AcceptedValuesRule",
    "DataQualityChecker",
    "NotNullRule",
    "RelationshipRule",
    "RuleBundle",
    "SQLiteRunLogger",
    "UniqueRule",
]

"""
Aqualisys: a Polars-first data quality toolkit.

Expose the key classes so downstream users can import from `aqualisys`.
"""

from .checker import DataQualityChecker, RuleBundle
from .checks.rules import AcceptedValuesRule, NotNullRule, RelationshipRule, UniqueRule
from .logging.sqlite import SQLiteRunLogger

__all__ = [
    "AcceptedValuesRule",
    "DataQualityChecker",
    "NotNullRule",
    "RelationshipRule",
    "RuleBundle",
    "SQLiteRunLogger",
    "UniqueRule",
]

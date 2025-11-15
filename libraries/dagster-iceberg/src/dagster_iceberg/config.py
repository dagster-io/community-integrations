"""Public API for Iceberg configuration classes.

This module re-exports configuration classes from the internal implementation
to maintain backward compatibility while avoiding circular dependencies.
"""

from dagster_iceberg._utils.config import (
    DEFAULT_PARTITION_FIELD_NAME_PREFIX,
    IcebergBranchConfig,
    IcebergCatalogConfig,
)

__all__ = [
    "DEFAULT_PARTITION_FIELD_NAME_PREFIX",
    "IcebergBranchConfig",
    "IcebergCatalogConfig",
]

from dagster._core.libraries import DagsterLibraryRegistry

from dagster_ducklake.ducklake import (
    DuckDBConfig,
    DuckLakeLocalDirectory,
    DuckLakeResource,
    PostgresConfig,
    S3Config,
    SqliteConfig,
)
from dagster_ducklake.duckdb_file import DuckDBFileResource
from dagster_ducklake.duckdb_protocol import DuckDBConnectionProvider

__all__ = [
    "DuckDBConfig",
    "DuckLakeLocalDirectory",
    "DuckLakeResource",
    "PostgresConfig",
    "S3Config",
    "SqliteConfig",
    "DuckDBFileResource",
    "DuckDBConnectionProvider",
]

__version__ = "0.0.1"

DagsterLibraryRegistry.register("dagster-duckdb", __version__, is_dagster_package=False)

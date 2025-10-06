from dagster._core.libraries import DagsterLibraryRegistry

from dagster_ducklake import (
    DuckDBFileResource as DuckDBFileResource,
    PostgresConfig as PostgresConfig,
    SqliteConfig as SqliteConfig,
    DuckDBConfig as DuckDBConfig,
    S3Config as S3Config,
    DuckLakeLocalDirectory as DuckLakeLocalDirectory,
    DuckLakeResource as DuckLakeResource,
)

__version__ = "0.0.1"

DagsterLibraryRegistry.register("dagster-duckdb", __version__, is_dagster_package=False)

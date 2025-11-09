from contextlib import contextmanager
from collections.abc import Generator

import dagster as dg
from duckdb import DuckDBPyConnection


class DuckDBConnectionProvider(dg.ConfigurableResource):
    """
    A protocol for Dagster resources that can provide a native DuckDB connection.

    This abstraction allows ops to depend on a generic connection provider,
    making them substitutable between different backends like a local file
    or a remote DuckLake instance.
    """

    @contextmanager
    def duckdb_connect(self) -> Generator[DuckDBPyConnection, None, None]:
        """
        A context manager that yields a pre-configured DuckDB connection.
        The connection is automatically closed upon exiting the context.
        """
        ...

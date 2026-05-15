from contextlib import contextmanager
from collections.abc import Generator

import duckdb  # pyrefly: ignore
from duckdb import DuckDBPyConnection  # pyrefly: ignore
from pydantic import Field

from .duckdb_protocol import DuckDBConnectionProvider


class DuckDBFileResource(DuckDBConnectionProvider):
    """
    A Dagster resource that provides a connection to a local DuckDB file.
    Implements the DuckDBConnectionProvider protocol.
    """

    file_path: str = Field(
        description="Path to the DuckDB database file. Use ':memory:' for an in-memory database."
    )
    read_only: bool = Field(
        default=True, description="Whether to open the database in read-only mode."
    )

    @contextmanager
    def duckdb_connect(self) -> Generator[DuckDBPyConnection, None, None]:
        """Yields a connection to the specified DuckDB file."""
        if self.file_path == ":memory:":
            conn = duckdb.connect(database=self.file_path)
        else:
            conn = duckdb.connect(database=self.file_path, read_only=self.read_only)
        try:
            yield conn
        finally:
            conn.close()

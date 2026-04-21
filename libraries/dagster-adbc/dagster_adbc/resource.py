from collections.abc import Mapping
from contextlib import contextmanager
from typing import Any, Generator

from adbc_driver_manager import dbapi
from dagster import ConfigurableResource
from pydantic import Field


class ADBCResource(ConfigurableResource):
    """Resource for interacting with a database via ADBC.
    Wraps an underlying adbc_driver_manager.dbapi connection.

    Examples:
        .. code-block:: python
        from dagster import Definitions, EnvVar, asset
        from dagster_adbc import ADBCResource


        @asset
        def my_table(dremio: ADBCResource) -> None:
            with dremio.get_connection() as connection, connection.cursor() as cursor:
                cursor.execute("SELECT * FROM my_table")
                table = cursor.fetch_arrow_table()


        defs = Definitions(
            assets=[my_table],
            resources={
                "dremio": ADBCResource(
                    driver="flightsql",
                    uri="grpc+tcp://localhost:32010",
                    db_kwargs={"username": "admin", "password": EnvVar("DREMIO_PASSWORD")},
                )
            },
        )
    """

    driver: str | None = Field(default=None, description="The driver to use.")
    uri: str | None = Field(
        default=None, description='The "uri" parameter to the database (if applicable).'
    )
    profile: str | None = Field(default=None, description="A connection profile to load.")
    entrypoint: str | None = Field(
        default=None,
        description="The driver-specific entrypoint, if different than the default.",
    )
    db_kwargs: Mapping[str, str] | None = Field(
        default=None,
        description="Key-value parameters to pass to the driver to initialize the database.",
    )
    conn_kwargs: Mapping[str, str] | None = Field(
        default=None,
        description="Key-value parameters to pass to the driver to initialize the connection.",
    )
    autocommit: bool = Field(default=False, description="Whether to enable autocommit.")

    @classmethod
    def _is_dagster_mainatained(cls) -> bool:
        return False

    @contextmanager
    def get_connection(self) -> Generator[dbapi.Connection, Any, None]:
        connection = dbapi.connect(
            driver=self.driver,
            uri=self.uri,
            profile=self.profile,
            entrypoint=self.entrypoint,
            db_kwargs=self.db_kwargs,
            conn_kwargs=self.conn_kwargs,
            autocommit=self.autocommit,
        )
        try:
            yield connection
        finally:
            connection.close()

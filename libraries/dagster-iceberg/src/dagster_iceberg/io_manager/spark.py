from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from typing import Optional, Union

try:
    from pyspark.sql.connect.session import SparkSession
    from pyspark.sql.connect.dataframe import DataFrame
except ImportError as e:
    raise ImportError("Please install dagster-iceberg with the 'spark' extra.") from e
from dagster import ConfigurableIOManagerFactory
from dagster._core.storage.db_io_manager import (
    DbClient,
    DbIOManager,
    DbTypeHandler,
    TableSlice,
)
from dagster._core.execution.context.input import InputContext
from dagster._core.execution.context.output import OutputContext
from dagster_pyspark.resources import spark_session_from_config


class SparkIcebergTypeHandler(DbTypeHandler[DataFrame]):
    """Type handler that reads and writes PySpark dataframes from and to Iceberg tables."""

    def handle_output(
        self,
        context: OutputContext,
        table_slice: TableSlice,
        obj: DataFrame,
        connection: SparkSession,
    ):
        """Writes a PySpark dataframe to an Iceberg table."""
        obj.writeTo(
            SparkIcebergDbClient.get_table_name(table_slice)  # pyright: ignore [reportArgumentType]
        ).create()  # TODO(deepyaman): Ensure table exists, and `overwritePartitions()`.

    def load_input(
        self, context: InputContext, table_slice: TableSlice, connection: SparkSession
    ) -> DataFrame:
        """Reads a PySpark dataframe from an Iceberg table."""
        return connection.table(SparkIcebergDbClient.get_table_name(table_slice))  # pyright: ignore [reportArgumentType]

    @property
    def supported_types(self) -> Sequence[type[object]]:
        return (DataFrame,)


class SparkIcebergDbClient(DbClient[SparkSession]):
    @staticmethod
    def delete_table_slice(
        context: OutputContext, table_slice: TableSlice, connection: SparkSession
    ) -> None: ...

    @staticmethod
    def get_select_statement(table_slice: TableSlice) -> str: ...

    @staticmethod
    def ensure_schema_exists(
        context: OutputContext, table_slice: TableSlice, connection: SparkSession
    ) -> None:
        """Ensures that the schema for an Iceberg table exists."""
        schema_name = (
            f"{table_slice.database}.{table_slice.schema}"
            if table_slice.database
            else table_slice.schema
        )
        connection.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    @staticmethod
    @contextmanager
    def connect(
        context: Union[OutputContext, InputContext], table_slice: TableSlice
    ) -> Iterator[SparkSession]:
        yield spark_session_from_config(
            context.resource_config.get("spark_config")
            if context.resource_config
            else None
        )


class SparkIcebergIOManager(ConfigurableIOManagerFactory):
    catalog_name: str
    namespace: str
    spark_config: Optional[Mapping[str, str]] = None

    def create_io_manager(self, context) -> DbIOManager:
        return DbIOManager(
            type_handlers=[SparkIcebergTypeHandler()],
            db_client=SparkIcebergDbClient(),
            database=self.catalog_name,
            schema=self.namespace,
            io_manager_name="SparkIcebergIOManager",
        )

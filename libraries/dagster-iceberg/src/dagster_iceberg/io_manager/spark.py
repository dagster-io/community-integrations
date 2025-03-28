from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, cast

try:
    from pyspark.sql.connect.dataframe import DataFrame
    from pyspark.sql.connect.session import SparkSession
except ImportError as e:
    raise ImportError("Please install dagster-iceberg with the 'spark' extra.") from e
from dagster import ConfigurableIOManagerFactory
from dagster._core.definitions.time_window_partitions import TimeWindow
from dagster._core.execution.context.input import InputContext
from dagster._core.execution.context.output import OutputContext
from dagster._core.storage.db_io_manager import (
    DbClient,
    DbIOManager,
    DbTypeHandler,
    TablePartitionDimension,
    TableSlice,
)
from pydantic import Field

if TYPE_CHECKING:
    from pyspark.sql._typing import OptionalPrimitiveType


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
        table_name = SparkIcebergDbClient.get_table_name(table_slice)
        table_exists = connection.catalog.tableExists(table_name)  # pyright: ignore [reportArgumentType]
        mode = "overwritePartitions" if table_exists else "create"
        getattr(obj.writeTo(table_name), mode)()  # pyright: ignore [reportArgumentType]

    def load_input(
        self,
        context: InputContext,
        table_slice: TableSlice,
        connection: SparkSession,
    ) -> DataFrame:
        """Reads a PySpark dataframe from an Iceberg table."""
        return connection.table(SparkIcebergDbClient.get_table_name(table_slice))  # pyright: ignore [reportArgumentType]

    @property
    def supported_types(self) -> Sequence[type[object]]:
        return (DataFrame,)


class SparkIcebergDbClient(DbClient[SparkSession]):
    @staticmethod
    def delete_table_slice(
        context: OutputContext,
        table_slice: TableSlice,
        connection: SparkSession,
    ) -> None: ...

    @staticmethod
    def get_select_statement(table_slice: TableSlice) -> str:
        col_str = ", ".join(table_slice.columns) if table_slice.columns else "*"

        if (
            table_slice.partition_dimensions
            and len(table_slice.partition_dimensions) > 0
        ):
            query = f"SELECT {col_str} FROM {table_slice.schema}.{table_slice.table} WHERE\n"
            return query + _partition_where_clause(table_slice.partition_dimensions)

        return f"""SELECT {col_str} FROM {table_slice.schema}.{table_slice.table}"""

    @staticmethod
    def ensure_schema_exists(
        context: OutputContext,
        table_slice: TableSlice,
        connection: SparkSession,
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
        context: OutputContext | InputContext,
        table_slice: TableSlice,
    ) -> Iterator[SparkSession]:
        builder = cast(SparkSession.Builder, SparkSession.builder)
        if context.resource_config is not None:
            if (spark_config := context.resource_config["spark_config"]) is not None:
                builder.config(
                    map=cast(dict[str, "OptionalPrimitiveType"], spark_config)
                )

            if (remote_url := context.resource_config["remote_url"]) is not None:
                builder.remote(cast(str, remote_url))

        yield builder.getOrCreate()


class SparkIcebergIOManager(ConfigurableIOManagerFactory):
    catalog_name: str
    namespace: str
    spark_config: dict[str, Any] | None = Field(default=None)
    remote_url: str | None = Field(default=None)

    def create_io_manager(self, context) -> DbIOManager:
        return DbIOManager(
            type_handlers=[SparkIcebergTypeHandler()],
            db_client=SparkIcebergDbClient(),
            database=self.catalog_name,
            schema=self.namespace,
            io_manager_name="SparkIcebergIOManager",
        )


def _partition_where_clause(
    partition_dimensions: Sequence[TablePartitionDimension],
) -> str:
    return " AND\n".join(
        (
            _time_window_where_clause(partition_dimension)
            if isinstance(partition_dimension.partitions, TimeWindow)
            else _static_where_clause(partition_dimension)
        )
        for partition_dimension in partition_dimensions
    )


def _time_window_where_clause(table_partition: TablePartitionDimension) -> str:
    partition = cast(TimeWindow, table_partition.partitions)
    start_dt, end_dt = partition
    start_dt_str = start_dt.isoformat()
    end_dt_str = end_dt.isoformat()
    return f"""{table_partition.partition_expr} >= TIMESTAMP '{start_dt_str}' AND {table_partition.partition_expr} < TIMESTAMP '{end_dt_str}'"""


def _static_where_clause(table_partition: TablePartitionDimension) -> str:
    partitions = ", ".join(f"'{partition}'" for partition in table_partition.partitions)
    return f"""{table_partition.partition_expr} in ({partitions})"""

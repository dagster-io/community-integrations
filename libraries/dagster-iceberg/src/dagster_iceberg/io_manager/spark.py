import re
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, cast

try:
    from pyspark.sql.connect.column import Column
    from pyspark.sql.connect.dataframe import DataFrame
    from pyspark.sql.connect.functions import col, lit
    from pyspark.sql.connect.session import SparkSession
    from pyspark.sql.functions import days, hours, months
except ImportError as e:
    raise ImportError("Please install dagster-iceberg with the 'spark' extra.") from e
from dagster import (
    ConfigurableIOManagerFactory,
    MultiPartitionsDefinition,
    PartitionsDefinition,
    TimeWindow,
)
from dagster._annotations import public
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

from dagster_iceberg._utils import preview

try:
    from dagster._core.definitions.partitions.schedule_type import (  # type: ignore[reportMissingImports]
        ScheduleType,
    )
except ImportError:
    from dagster._core.definitions.partition import (  # type: ignore[reportMissingImports]
        ScheduleType,
    )

if TYPE_CHECKING:
    from pyspark.sql._typing import OptionalPrimitiveType


@preview
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
        table_exists = connection.catalog.tableExists(table_name)
        writer = obj.writeTo(table_name)
        mode = "overwritePartitions" if table_exists else "create"
        if table_slice.partition_dimensions and mode == "create":
            partition_transforms = _partition_transforms(context.asset_partitions_def)
            writer = writer.partitionedBy(
                *[
                    partition_transform(partition_dimension.partition_expr)
                    for partition_transform, partition_dimension in zip(
                        partition_transforms, table_slice.partition_dimensions
                    )
                ]
            )

        getattr(writer, mode)()

    def _load_table(
        self, table_slice: TableSlice, connection: SparkSession
    ) -> DataFrame:
        """Reads a PySpark dataframe from an Iceberg table."""
        # Spark parses the table name only as an identifier and receives columns and
        # partition keys as column references and literal values, so none can add SQL.
        frame = connection.table(f"{table_slice.schema}.{table_slice.table}")
        for partition_dimension in table_slice.partition_dimensions or []:
            column = col(partition_dimension.partition_expr)
            if isinstance(partition_dimension.partitions, TimeWindow):
                start_dt, end_dt = partition_dimension.partitions
                frame = frame.where(
                    (column >= _timestamp(start_dt)) & (column < _timestamp(end_dt))
                )
            else:
                frame = frame.where(column.isin(list(partition_dimension.partitions)))
        if table_slice.columns:
            return frame.select(*(col(column) for column in table_slice.columns))
        return frame

    def load_input(
        self,
        context: InputContext,
        table_slice: TableSlice,
        connection: SparkSession,
    ) -> DataFrame:
        """Reads a PySpark dataframe from an Iceberg table."""
        return self._load_table(table_slice, connection)

    @property
    def supported_types(self) -> Sequence[type[object]]:
        return (DataFrame,)


@preview
class SparkIcebergDbClient(DbClient[SparkSession]):
    @staticmethod
    def delete_table_slice(
        context: OutputContext,
        table_slice: TableSlice,
        connection: SparkSession,
    ) -> None: ...

    @staticmethod
    def get_select_statement(table_slice: TableSlice) -> str:
        col_str = (
            ", ".join(_display_name(column) for column in table_slice.columns)
            if table_slice.columns
            else "*"
        )
        table = (
            f"{_display_name(table_slice.schema)}.{_display_name(table_slice.table)}"
        )

        if table_slice.partition_dimensions:
            query = f"SELECT {col_str} FROM {table} WHERE\n"
            return query + _partition_where_clause(table_slice.partition_dimensions)

        return f"""SELECT {col_str} FROM {table}"""

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
        # Spark has no DataFrame API for creating a namespace, so each part of the name
        # is quoted. Spark expands ${...} even inside quotes, so such names are refused.
        if "${" in schema_name:
            raise ValueError(
                f"Schema {schema_name!r} contains '${{', which Spark would substitute"
            )
        quoted = ".".join(_quote_name(part) for part in schema_name.split("."))
        connection.sql(f"CREATE SCHEMA IF NOT EXISTS {quoted}")

    @staticmethod
    @contextmanager
    def connect(
        context: OutputContext | InputContext,
        table_slice: TableSlice,
    ) -> Iterator[SparkSession]:
        builder = cast("SparkSession.Builder", SparkSession.builder)
        if context.resource_config is not None:
            if (spark_config := context.resource_config["spark_config"]) is not None:
                builder.config(
                    map=cast("dict[str, OptionalPrimitiveType]", spark_config)
                )

            if (remote_url := context.resource_config["remote_url"]) is not None:
                builder.remote(cast("str", remote_url))

        yield builder.getOrCreate()


@public
@preview
class SparkIcebergIOManager(ConfigurableIOManagerFactory):
    """An I/O manager definition that reads inputs from and writes outputs to Iceberg tables using PySpark.

    This I/O manager is only designed to work with Spark Connect.

    Example:
        .. code-block:: python

            from dagster import Definitions, asset
            from dagster_iceberg.io_manager.spark import SparkIcebergIOManager
            from pyspark.sql import SparkSession
            from pyspark.sql.connect.dataframe import DataFrame

            resources = {
                "io_manager": SparkIcebergIOManager(
                    catalog_name="test",
                    namespace="dagster",
                    remote_url="spark://localhost",
                )
            }


            @asset
            def iris_dataset() -> DataFrame:
                spark = SparkSession.builder.remote("sc://localhost").getOrCreate()
                return spark.read.csv(
                    "https://docs.dagster.io/assets/iris.csv",
                    schema=(
                        "sepal_length_cm FLOAT, "
                        "sepal_width_cm FLOAT, "
                        "petal_length_cm FLOAT, "
                        "petal_width_cm FLOAT, "
                        "species STRING"
                    ),
                )


            defs = Definitions(assets=[iris_dataset], resources=resources)

    """

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


def _partition_transforms(partitions_def: PartitionsDefinition) -> Sequence[Callable]:
    if isinstance(partitions_def, MultiPartitionsDefinition):
        return [
            _partition_transforms(x.partitions_def)[0]
            for x in partitions_def.partitions_defs
        ]

    schedule_type = getattr(partitions_def, "schedule_type", None)
    if schedule_type is ScheduleType.HOURLY:
        return [hours]
    if schedule_type is ScheduleType.DAILY:
        return [days]
    if schedule_type is ScheduleType.WEEKLY:
        return [days]
    if schedule_type is ScheduleType.MONTHLY:
        return [months]
    return [lambda x: x]


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
    partition = cast("TimeWindow", table_partition.partitions)
    start_dt, end_dt = partition
    start_dt_str = start_dt.isoformat()
    end_dt_str = end_dt.isoformat()
    column = _display_name(table_partition.partition_expr)
    return f"""{column} >= TIMESTAMP '{start_dt_str}' AND {column} < TIMESTAMP '{end_dt_str}'"""


def _timestamp(value: Any) -> Column:
    # Casting the ISO string matches how Spark reads a TIMESTAMP '...' literal.
    return lit(value.isoformat()).cast("timestamp")


# get_select_statement's text is only recorded as the "Query" output metadata; reads
# never run it. Keys are escaped for Spark's default parser settings so the text
# reads as the query a user would type.
_SPARK_LITERAL_ESCAPES = str.maketrans({"\\": "\\\\", "'": "\\'"})


def _static_where_clause(table_partition: TablePartitionDimension) -> str:
    partitions = ", ".join(
        f"'{partition.translate(_SPARK_LITERAL_ESCAPES)}'"
        for partition in table_partition.partitions
    )
    return f"""{_display_name(table_partition.partition_expr)} in ({partitions})"""


def _quote_name(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


_PLAIN_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _display_name(name: str) -> str:
    # Dots separate namespace and nested field names, as they do for Spark's col().
    return ".".join(
        part if _PLAIN_NAME.fullmatch(part) else _quote_name(part)
        for part in name.split(".")
    )

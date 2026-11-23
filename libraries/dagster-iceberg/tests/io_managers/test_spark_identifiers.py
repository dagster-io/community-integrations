"""Table, column, and partition names reach Spark as identifiers, never as SQL."""

import datetime as dt
from collections.abc import Iterator

import pytest
from dagster import TimeWindow, build_input_context, build_output_context
from dagster._core.storage.db_io_manager import TablePartitionDimension, TableSlice
from pyiceberg.catalog import Catalog
from pyspark.errors import ParseException
from pyspark.sql import SparkSession

from dagster_iceberg.io_manager.spark import (
    SparkIcebergDbClient,
    SparkIcebergTypeHandler,
)

REGION = "it's region"
EVENT_TIME = "event time"
COLUMNS = [
    "plain",
    "MixedCase",
    "my col",
    "it's",
    "x; DROP TABLE t --",
    "/* comment */",
    "${env:HOME}",
]
ROWS = [
    ("east", dt.datetime(2024, 1, 1, 12)),
    ("west", dt.datetime(2024, 1, 2, 12)),
]


# The Spark Connect server in the compose stack runs the queries, so the test does
# not depend on a JVM on the machine running pytest.
@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    session = SparkSession.builder.remote("sc://localhost").getOrCreate()
    session.createDataFrame(
        [
            (region, event_time, *(f"{column}/{region}" for column in COLUMNS))
            for region, event_time in ROWS
        ],
        [REGION, EVENT_TIME, *COLUMNS],
    ).createOrReplaceGlobalTempView("identifier_rows")
    yield session
    session.catalog.dropGlobalTempView("identifier_rows")
    session.stop()


def load(
    spark: SparkSession,
    partitions: list[str] | TimeWindow,
    partition_expr: str = REGION,
    columns: list[str] | None = None,
    table: str = "identifier_rows",
) -> list[dict]:
    table_slice = TableSlice(
        table=table,
        schema="global_temp",
        columns=columns,
        partition_dimensions=[
            TablePartitionDimension(
                partition_expr=partition_expr, partitions=partitions
            )
        ],
    )
    frame = SparkIcebergTypeHandler().load_input(
        build_input_context(), table_slice, spark
    )
    return [row.asDict() for row in frame.collect()]


@pytest.mark.parametrize("column", COLUMNS)
def test_loads_the_named_column_of_the_partition(spark: SparkSession, column: str):
    assert load(spark, ["east"], columns=[column]) == [{column: f"{column}/east"}]


def test_time_window_partition_filters_on_the_named_column(spark: SparkSession):
    window = TimeWindow(
        dt.datetime(2024, 1, 2, tzinfo=dt.UTC),
        dt.datetime(2024, 1, 3, tzinfo=dt.UTC),
    )
    rows = load(spark, window, partition_expr=EVENT_TIME, columns=[REGION])
    assert rows == [{REGION: "west"}]


@pytest.mark.parametrize(
    "table", ["identifier_rows WHERE false --", "identifier_rows LIMIT 0"]
)
def test_table_name_cannot_add_sql(spark: SparkSession, table: str):
    with pytest.raises(ParseException):
        load(spark, ["plain/east"], partition_expr="plain", table=table)


# The Iceberg catalog accepts any namespace name; Spark's own catalog does not.
@pytest.fixture
def created_schema(catalog: Catalog) -> Iterator[str]:
    schema = "it's; drop schema default --"
    yield schema
    catalog.drop_namespace(schema)


def test_ensure_schema_exists_creates_the_named_schema(
    spark: SparkSession, catalog: Catalog, created_schema: str
):
    table_slice = TableSlice(table="t", schema=created_schema, database="postgres")
    SparkIcebergDbClient.ensure_schema_exists(
        build_output_context(), table_slice, spark
    )
    assert (created_schema,) in catalog.list_namespaces()


def test_ensure_schema_exists_rejects_variable_substitution(spark: SparkSession):
    table_slice = TableSlice(table="t", schema="${spark.app.name}", database="postgres")
    with pytest.raises(ValueError, match="substitut"):
        SparkIcebergDbClient.ensure_schema_exists(
            build_output_context(), table_slice, spark
        )

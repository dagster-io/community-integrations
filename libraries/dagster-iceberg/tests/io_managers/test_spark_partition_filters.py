"""Static partition keys reach the Spark SQL filter as their exact values."""

from collections.abc import Iterator

import pytest
from dagster import build_input_context
from dagster._core.storage.db_io_manager import TablePartitionDimension, TableSlice
from pyspark.sql import SparkSession

from dagster_iceberg.io_manager.spark import SparkIcebergTypeHandler

KEYS = [
    "plain",
    "it's",
    "north\\west",
    "it's\\x",
    "x') OR 1=1 --",
    "x\\') OR 1=1 --",
    "${spark.app.name}",
]


# The Spark Connect server in the compose stack runs the filter, so the test does
# not depend on a JVM on the machine running pytest.
@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    session = SparkSession.builder.remote("sc://localhost").getOrCreate()
    session.createDataFrame(
        [(key, position) for position, key in enumerate(KEYS)], ["key", "position"]
    ).createOrReplaceGlobalTempView("partition_keys")
    yield session
    session.catalog.dropGlobalTempView("partition_keys")
    session.stop()


def keys_slice(*keys: str, columns: list[str] | None = None) -> TableSlice:
    return TableSlice(
        table="partition_keys",
        schema="global_temp",
        columns=columns,
        partition_dimensions=[
            TablePartitionDimension(partition_expr="key", partitions=list(keys))
        ],
    )


def load_keys(spark: SparkSession, table_slice: TableSlice) -> list[str]:
    frame = SparkIcebergTypeHandler().load_input(
        build_input_context(), table_slice, spark
    )
    return sorted(row.key for row in frame.collect())


# Spark reads string literals differently when this setting is on, so every filter
# test runs with it both off and on.
ESCAPED_STRING_LITERALS = "spark.sql.parser.escapedStringLiterals"


@pytest.fixture(params=["false", "true"])
def escaped_string_literals(
    spark: SparkSession, request: pytest.FixtureRequest
) -> Iterator[None]:
    original = spark.conf.get(ESCAPED_STRING_LITERALS)
    spark.conf.set(ESCAPED_STRING_LITERALS, request.param)
    yield
    spark.conf.set(ESCAPED_STRING_LITERALS, original)


@pytest.mark.usefixtures("escaped_string_literals")
@pytest.mark.parametrize("key", KEYS)
def test_static_partition_selects_only_that_key(spark: SparkSession, key: str):
    assert load_keys(spark, keys_slice(key)) == [key]


@pytest.mark.usefixtures("escaped_string_literals")
def test_static_partition_selects_several_keys(spark: SparkSession):
    assert load_keys(spark, keys_slice("plain", "it's", "north\\west")) == [
        "it's",
        "north\\west",
        "plain",
    ]


@pytest.mark.usefixtures("escaped_string_literals")
def test_static_partition_loads_only_the_selected_columns(spark: SparkSession):
    frame = SparkIcebergTypeHandler().load_input(
        build_input_context(), keys_slice("it's", columns=["position"]), spark
    )
    assert [row.asDict() for row in frame.collect()] == [{"position": 1}]

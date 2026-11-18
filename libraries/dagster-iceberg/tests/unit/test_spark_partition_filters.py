"""Static partition keys reach the Spark SQL filter as literal values."""

from collections.abc import Iterator

import pytest
from dagster import build_input_context
from dagster._core.storage.db_io_manager import TablePartitionDimension, TableSlice
from pyspark.sql import SparkSession

from dagster_iceberg.io_manager.spark import SparkIcebergTypeHandler

KEYS = ["plain", "it's", "back\\slash", "x') OR 1=1 --", "x\\') OR 1=1 --"]


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    session = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.createDataFrame(
        [(key,) for key in KEYS], ["key"]
    ).createOrReplaceGlobalTempView("keys")
    yield session
    session.stop()


def keys_slice(*keys: str) -> TableSlice:
    return TableSlice(
        table="keys",
        schema="global_temp",
        partition_dimensions=[
            TablePartitionDimension(partition_expr="key", partitions=list(keys))
        ],
    )


def load_keys(spark: SparkSession, table_slice: TableSlice) -> list[str]:
    frame = SparkIcebergTypeHandler().load_input(
        build_input_context(), table_slice, spark
    )
    return sorted(row.key for row in frame.collect())


@pytest.mark.parametrize("key", KEYS)
def test_static_partition_selects_only_that_key(spark: SparkSession, key: str):
    assert load_keys(spark, keys_slice(key)) == [key]


def test_static_partition_selects_several_keys(spark: SparkSession):
    assert load_keys(spark, keys_slice("plain", "it's")) == ["it's", "plain"]

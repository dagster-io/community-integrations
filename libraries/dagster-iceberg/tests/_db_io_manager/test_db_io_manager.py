"""
We only support very specific cases of partition mappings

TODO:
- Single partitioned to single partitioned
- Single partitioned to non-partitioned
- Non-partitioned to single partitioned
"""

import datetime as dt

import pyarrow as pa
import pytest
from dagster import (
    AssetExecutionContext,
    AssetIn,
    DailyPartitionsDefinition,
    DimensionPartitionMapping,
    MultiPartitionMapping,
    MultiPartitionsDefinition,
    MultiToSingleDimensionPartitionMapping,
    SpecificPartitionsPartitionMapping,
    StaticPartitionMapping,
    StaticPartitionsDefinition,
    asset,
    materialize,
)

from dagster_iceberg.config import IcebergCatalogConfig
from dagster_iceberg.io_manager.arrow import PyArrowIcebergIOManager


@pytest.fixture
def io_manager(
    catalog_name: str,
    namespace: str,
    catalog_config_properties: dict[str, str],
) -> PyArrowIcebergIOManager:
    return PyArrowIcebergIOManager(
        name=catalog_name,
        config=IcebergCatalogConfig(properties=catalog_config_properties),
        namespace=namespace,
        db_io_manager="custom",
    )


daily_partitions_def = DailyPartitionsDefinition(
    start_date="2022-01-01",
    end_date="2022-01-10",
)

letter_partitions_def = StaticPartitionsDefinition(["a", "b", "c"])

color_partitions_def = StaticPartitionsDefinition(["red", "blue", "yellow"])

multi_partition_with_letter = MultiPartitionsDefinition(
    partitions_defs={
        "date": daily_partitions_def,
        "letter": letter_partitions_def,
    },
)

multi_partition_with_color = MultiPartitionsDefinition(
    partitions_defs={
        "date": daily_partitions_def,
        "color": color_partitions_def,
    },
)


@asset(
    key_prefix=["my_schema"],
)
def asset_1() -> pa.Table:
    return pa.Table.from_pydict(
        {
            "value": [1],
            "b": [1],
        },
    )


@asset(
    key_prefix=["my_schema"],
)
def asset_2(asset_1: pa.Table) -> pa.Table:
    return asset_1


# case: we have multiple partitions
@asset(
    key_prefix=["my_schema"],
    partitions_def=multi_partition_with_color,
    metadata={
        "partition_expr": {
            "date": "date_column",
            "color": "color_column",
        },
    },
)
def multi_partitioned_asset_1(context: AssetExecutionContext) -> pa.Table:
    color, date = context.partition_key.split("|")
    date_parsed = dt.datetime.strptime(date, "%Y-%m-%d").date()

    return pa.Table.from_pydict(
        {
            "date_column": [date_parsed],
            "value": [1],
            "b": [1],
            "color_column": [color],
        },
    )


# Multi-to-multi asset is supported
@asset(
    key_prefix=["my_schema"],
    partitions_def=multi_partition_with_color,
    metadata={
        "partition_expr": {
            "date": "date_column",
            "color": "color_column",
        },
    },
)
def multi_partitioned_asset_2(multi_partitioned_asset_1: pa.Table) -> pa.Table:
    return multi_partitioned_asset_1


@asset(
    key_prefix=["my_schema"],
)
def non_partitioned_asset(multi_partitioned_asset_1: pa.Table) -> pa.Table:
    return multi_partitioned_asset_1


# Multi-to-single asset is supported through MultiToSingleDimensionPartitionMapping
@asset(
    key_prefix=["my_schema"],
    partitions_def=daily_partitions_def,
    ins={
        "multi_partitioned_asset": AssetIn(
            ["my_schema", "multi_partitioned_asset_1"],
            partition_mapping=MultiToSingleDimensionPartitionMapping(
                partition_dimension_name="date",
            ),
        ),
    },
    metadata={
        "partition_expr": "date_column",
    },
)
def single_partitioned_asset_date(multi_partitioned_asset: pa.Table) -> pa.Table:
    return multi_partitioned_asset


@asset(
    key_prefix=["my_schema"],
    partitions_def=color_partitions_def,
    ins={
        "multi_partitioned_asset": AssetIn(
            ["my_schema", "multi_partitioned_asset_1"],
            partition_mapping=MultiToSingleDimensionPartitionMapping(
                partition_dimension_name="color",
            ),
        ),
    },
    metadata={
        "partition_expr": "color_column",
    },
)
def single_partitioned_asset_color(multi_partitioned_asset: pa.Table) -> pa.Table:
    return multi_partitioned_asset


@asset(
    partitions_def=multi_partition_with_letter,
    key_prefix=["my_schema"],
    metadata={"partition_expr": {"time": "time", "letter": "letter"}},
    ins={
        "multi_partitioned_asset": AssetIn(
            ["my_schema", "multi_partitioned_asset_1"],
            partition_mapping=MultiPartitionMapping(
                {
                    "color": DimensionPartitionMapping(
                        dimension_name="letter",
                        partition_mapping=StaticPartitionMapping(
                            {"blue": "a", "red": "b", "yellow": "c"},
                        ),
                    ),
                    "date": DimensionPartitionMapping(
                        dimension_name="date",
                        partition_mapping=SpecificPartitionsPartitionMapping(
                            ["2022-01-01", "2024-01-01"],
                        ),
                    ),
                },
            ),
        ),
    },
)
def mapped_multi_partition(
    context: AssetExecutionContext,
    multi_partitioned_asset: pa.Table,
) -> pa.Table:
    letter, _ = context.partition_key.split("|")

    return multi_partitioned_asset.append_column("letter", pa.array([letter]))


def test_unpartitioned_asset_to_unpartitioned_asset(
    io_manager: PyArrowIcebergIOManager,
):
    resource_defs = {"io_manager": io_manager}

    res = materialize([asset_1, asset_2], resources=resource_defs)
    assert res.success


def test_multi_partitioned_to_multi_partitioned_asset(
    io_manager: PyArrowIcebergIOManager,
):
    resource_defs = {"io_manager": io_manager}

    for partition_key in ["red|2022-01-01", "red|2022-01-02", "red|2022-01-03"]:
        res = materialize(
            [multi_partitioned_asset_1, multi_partitioned_asset_2],
            partition_key=partition_key,
            resources=resource_defs,
        )
        assert res.success


def test_multi_partitioned_to_single_partitioned_asset_colors(
    io_manager: PyArrowIcebergIOManager,
):
    resource_defs = {"io_manager": io_manager}

    for partition_key in ["red|2022-01-01", "blue|2022-01-01", "yellow|2022-01-01"]:
        res = materialize(
            [multi_partitioned_asset_1],
            partition_key=partition_key,
            resources=resource_defs,
        )
        assert res.success
    res = materialize(
        [multi_partitioned_asset_1, single_partitioned_asset_date],
        partition_key="2022-01-01",
        resources=resource_defs,
        selection=[single_partitioned_asset_date],
    )
    assert res.success


def test_multi_partitioned_to_single_partitioned_asset_dates(
    io_manager: PyArrowIcebergIOManager,
):
    resource_defs = {"io_manager": io_manager}

    for partition_key in ["red|2022-01-01", "red|2022-01-02", "red|2022-01-03"]:
        res = materialize(
            [multi_partitioned_asset_1],
            partition_key=partition_key,
            resources=resource_defs,
        )
        assert res.success
    res = materialize(
        [multi_partitioned_asset_1, single_partitioned_asset_color],
        partition_key="red",
        resources=resource_defs,
        selection=[single_partitioned_asset_color],
    )
    assert res.success


def test_multi_partitioned_to_non_partitioned_asset(
    io_manager: PyArrowIcebergIOManager,
):
    resource_defs = {"io_manager": io_manager}

    for partition_key in ["red|2022-01-01", "red|2022-01-02", "red|2022-01-03"]:
        res = materialize(
            [multi_partitioned_asset_1],
            partition_key=partition_key,
            resources=resource_defs,
        )
        assert res.success
    res = materialize(
        [multi_partitioned_asset_1, non_partitioned_asset],
        resources=resource_defs,
        selection=[non_partitioned_asset],
    )
    assert res.success


def test_multi_partitioned_to_multi_partitioned_with_different_dimensions(
    io_manager: PyArrowIcebergIOManager,
):
    resource_defs = {"io_manager": io_manager}

    for partition_key in ["red|2022-01-01", "blue|2022-01-01", "yellow|2022-01-01"]:
        res = materialize(
            [multi_partitioned_asset_1],
            partition_key=partition_key,
            resources=resource_defs,
        )
        assert res.success
    with pytest.raises(ValueError, match="Could not map partition to partition expr"):
        res = materialize(
            [multi_partitioned_asset_1, mapped_multi_partition],
            partition_key="2022-01-01|a",
            resources=resource_defs,
            selection=[mapped_multi_partition],
        )

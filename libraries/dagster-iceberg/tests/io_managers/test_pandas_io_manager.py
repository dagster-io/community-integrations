import datetime as dt

import pandas as pd
import pyarrow as pa
import pytest
from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    HourlyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    asset,
    materialize,
)
from pyiceberg.catalog import Catalog

from dagster_iceberg.config import IcebergCatalogConfig
from dagster_iceberg.io_manager.pandas import PandasIcebergIOManager


@pytest.fixture
def io_manager(
    catalog_name: str,
    namespace: str,
    catalog_config_properties: dict[str, str],
) -> PandasIcebergIOManager:
    return PandasIcebergIOManager(
        name=catalog_name,
        config=IcebergCatalogConfig(properties=catalog_config_properties),
        namespace=namespace,
    )


@pytest.fixture
def custom_db_io_manager(io_manager: PandasIcebergIOManager):
    return io_manager.create_io_manager(None)


# NB: iceberg table identifiers are namespace + asset names (see below)
@pytest.fixture
def asset_b_df_table_identifier(namespace: str) -> str:
    return f"{namespace}.b_df"


@pytest.fixture
def asset_b_plus_one_table_identifier(namespace: str) -> str:
    return f"{namespace}.b_plus_one"


@pytest.fixture
def asset_hourly_partitioned_table_identifier(namespace: str) -> str:
    return f"{namespace}.hourly_partitioned"


@pytest.fixture
def asset_daily_partitioned_table_identifier(namespace: str) -> str:
    return f"{namespace}.daily_partitioned"


@pytest.fixture
def asset_multi_partitioned_table_identifier(namespace: str) -> str:
    return f"{namespace}.multi_partitioned"


@asset(key_prefix=["my_schema"])
def b_df() -> pd.DataFrame:
    return pa.Table.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]}).to_pandas()


@asset(key_prefix=["my_schema"])
def b_plus_one(b_df: pd.DataFrame) -> pd.DataFrame:
    b_df["a"] = b_df["a"] + 1
    return b_df


@asset(
    key_prefix=["my_schema"],
    partitions_def=HourlyPartitionsDefinition(start_date=dt.datetime(2022, 1, 1, 0)),
    config_schema={"value": str},
    metadata={"partition_expr": "partition"},
)
def hourly_partitioned(context: AssetExecutionContext) -> pd.DataFrame:
    partition = dt.datetime.strptime(context.partition_key, "%Y-%m-%d-%H:%M")
    value = context.op_execution_context.op_config["value"]

    return pa.Table.from_pydict(
        {"partition": [partition], "value": [value], "b": [1]},
    ).to_pandas()


@asset(
    key_prefix=["my_schema"],
    partitions_def=DailyPartitionsDefinition(start_date="2022-01-01"),
    config_schema={"value": str},
    metadata={"partition_expr": "partition"},
)
def daily_partitioned(context: AssetExecutionContext) -> pd.DataFrame:
    partition = dt.datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    value = context.op_execution_context.op_config["value"]

    return pa.Table.from_pydict(
        {"partition": [partition], "value": [value], "b": [1]},
    ).to_pandas()


@asset(
    key_prefix=["my_schema"],
    partitions_def=MultiPartitionsDefinition(
        partitions_defs={
            "date": DailyPartitionsDefinition(
                start_date="2022-01-01",
                end_date="2022-01-10",
            ),
            "category": StaticPartitionsDefinition(["a", "b", "c"]),
        },
    ),
    config_schema={"value": str},
    metadata={
        "partition_expr": {
            "date": "date_this",
            "category": "category_this",
        },
    },
)
def multi_partitioned(context: AssetExecutionContext) -> pd.DataFrame:
    category, date = context.partition_key.split("|")
    date_parsed = dt.datetime.strptime(date, "%Y-%m-%d").date()
    value = context.op_execution_context.op_config["value"]

    return pa.Table.from_pydict(
        {
            "date_this": [date_parsed],
            "value": [value],
            "b": [1],
            "category_this": [category],
        },
    ).to_pandas()


def test_iceberg_io_manager_with_assets(
    asset_b_df_table_identifier: str,
    asset_b_plus_one_table_identifier: str,
    catalog: Catalog,
    io_manager: PandasIcebergIOManager,
):
    resource_defs = {"io_manager": io_manager}

    for _ in range(2):
        res = materialize([b_df, b_plus_one], resources=resource_defs)
        assert res.success

        table = catalog.load_table(asset_b_df_table_identifier)
        out_df = table.scan().to_arrow()
        assert out_df["a"].to_pylist() == [1, 2, 3]

        dt = catalog.load_table(asset_b_plus_one_table_identifier)
        out_dt = dt.scan().to_arrow()
        assert out_dt["a"].to_pylist() == [2, 3, 4]


def test_iceberg_io_manager_with_daily_partitioned_assets(
    asset_daily_partitioned_table_identifier: str,
    catalog: Catalog,
    io_manager: PandasIcebergIOManager,
):
    resource_defs = {"io_manager": io_manager}

    for date in ["2022-01-01", "2022-01-02", "2022-01-03"]:
        res = materialize(
            [daily_partitioned],
            partition_key=date,
            resources=resource_defs,
            run_config={
                "ops": {"my_schema__daily_partitioned": {"config": {"value": "1"}}},
            },
        )
        assert res.success

    table = catalog.load_table(asset_daily_partitioned_table_identifier)
    assert len(table.spec().fields) == 1
    assert table.spec().fields[0].name == "partition"

    out_df = table.scan().to_arrow()
    assert out_df["partition"].to_pylist() == [
        dt.date(2022, 1, 3),
        dt.date(2022, 1, 2),
        dt.date(2022, 1, 1),
    ]


def test_iceberg_io_manager_with_hourly_partitioned_assets(
    asset_hourly_partitioned_table_identifier: str,
    catalog: Catalog,
    io_manager: PandasIcebergIOManager,
):
    resource_defs = {"io_manager": io_manager}

    for date in ["2022-01-01-01:00", "2022-01-01-02:00", "2022-01-01-03:00"]:
        res = materialize(
            [hourly_partitioned],
            partition_key=date,
            resources=resource_defs,
            run_config={
                "ops": {"my_schema__hourly_partitioned": {"config": {"value": "1"}}},
            },
        )
        assert res.success

    table = catalog.load_table(asset_hourly_partitioned_table_identifier)
    assert len(table.spec().fields) == 1
    assert table.spec().fields[0].name == "partition"

    out_df = table.scan().to_arrow()
    assert out_df["partition"].to_pylist() == [
        dt.datetime(2022, 1, 1, 3, 0),
        dt.datetime(2022, 1, 1, 2, 0),
        dt.datetime(2022, 1, 1, 1, 0),
    ]


def test_iceberg_io_manager_with_multipartitioned_assets(
    asset_multi_partitioned_table_identifier: str,
    catalog: Catalog,
    io_manager: PandasIcebergIOManager,
):
    resource_defs = {"io_manager": io_manager}

    for key in [
        "a|2022-01-01",
        "b|2022-01-01",
        "c|2022-01-01",
        "a|2022-01-02",
        "b|2022-01-02",
        "c|2022-01-02",
    ]:
        res = materialize(
            [multi_partitioned],
            partition_key=key,
            resources=resource_defs,
            run_config={
                "ops": {"my_schema__multi_partitioned": {"config": {"value": "1"}}},
            },
        )
        assert res.success

    table = catalog.load_table(asset_multi_partitioned_table_identifier)
    assert len(table.spec().fields) == 2
    assert [f.name for f in table.spec().fields] == ["category_this", "date_this"]

    out_df = table.scan().to_arrow()
    assert out_df["date_this"].to_pylist() == [
        dt.date(2022, 1, 2),
        dt.date(2022, 1, 2),
        dt.date(2022, 1, 2),
        dt.date(2022, 1, 1),
        dt.date(2022, 1, 1),
        dt.date(2022, 1, 1),
    ]
    assert out_df["category_this"].to_pylist() == ["c", "b", "a", "c", "b", "a"]

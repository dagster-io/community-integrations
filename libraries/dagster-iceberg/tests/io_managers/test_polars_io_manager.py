import datetime

import polars as pl
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
from dagster_iceberg.io_manager.polars import PolarsIcebergIOManager


@pytest.fixture
def io_manager(
    catalog_name: str,
    namespace: str,
    catalog_config_properties: dict[str, str],
) -> PolarsIcebergIOManager:
    return PolarsIcebergIOManager(
        name=catalog_name,
        config=IcebergCatalogConfig(properties=catalog_config_properties),
        namespace=namespace,
    )


# NB: iceberg table identifiers are namespace + asset names (see below)
@pytest.fixture
def asset_b_df_table_identifier(namespace: str) -> str:
    return f"{namespace}.b_df"


@pytest.fixture
def asset_b_plus_one_table_identifier(namespace: str) -> str:
    return f"{namespace}.b_plus_one"


@pytest.fixture
def asset_daily_partitioned_table_identifier(namespace: str) -> str:
    return f"{namespace}.daily_partitioned"


@pytest.fixture
def asset_hourly_partitioned_table_identifier(namespace: str) -> str:
    return f"{namespace}.hourly_partitioned"


@pytest.fixture
def asset_multi_partitioned_table_identifier(namespace: str) -> str:
    return f"{namespace}.multi_partitioned"


@pytest.fixture
def asset_daily_partitioned_append_mode_table_identifier(namespace: str) -> str:
    return f"{namespace}.daily_partitioned_append_mode"


@pytest.fixture
def asset_multi_partitioned_append_mode_table_identifier(namespace: str) -> str:
    return f"{namespace}.multi_partitioned_append_mode"


@pytest.fixture
def asset_multi_partitioned_overwrite_mode_table_identifier(namespace: str) -> str:
    return f"{namespace}.multi_partitioned_overwrite_mode"


@asset(
    key_prefix=["my_schema"],
    metadata={"partition_spec_update_mode": "update", "schema_update_mode": "update"},
)
def b_df() -> pl.LazyFrame:
    pdf = pl.from_dict({"a": [1, 2, 3], "b": [4, 5, 6]})
    return pdf.lazy()


@asset(key_prefix=["my_schema"])
def b_plus_one(b_df: pl.LazyFrame) -> pl.LazyFrame:
    return b_df.with_columns(a=pl.col("a") + 1)


@asset(
    key_prefix=["my_schema"],
    partitions_def=DailyPartitionsDefinition(start_date="2022-01-01"),
    config_schema={"value": str},
    metadata={"partition_expr": "partition"},
)
def daily_partitioned(context: AssetExecutionContext) -> pl.DataFrame:
    partition = datetime.datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    value = context.op_execution_context.op_config["value"]

    return pl.from_dict({"partition": [partition], "value": [value], "b": [1]})


@asset(
    key_prefix=["my_schema"],
    partitions_def=DailyPartitionsDefinition(start_date="2022-01-01"),
    config_schema={"value": str},
    metadata={"partition_expr": "partition", "write_mode": "overwrite"},
)
def daily_partitioned_append_mode(context: AssetExecutionContext) -> pl.DataFrame:
    partition = datetime.datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    value = context.op_execution_context.op_config["value"]

    context.add_output_metadata({"write_mode": "append"})
    return pl.from_dict({"partition": [partition], "value": [value], "b": [1]})


@asset(
    key_prefix=["my_schema"],
    partitions_def=HourlyPartitionsDefinition(
        start_date=datetime.datetime(2022, 1, 1, 0)
    ),
    config_schema={"value": str},
    metadata={"partition_expr": "partition", "write_mode": "append"},
)
def hourly_partitioned_append_mode(
    context: AssetExecutionContext,
) -> pl.DataFrame:
    partition = datetime.datetime.strptime(context.partition_key, "%Y-%m-%d-%H:%M")
    value = context.op_execution_context.op_config["value"]

    return pl.from_dict({"partition": [partition], "value": [value], "b": [1]})


@asset(
    key_prefix=["my_schema"],
    partitions_def=HourlyPartitionsDefinition(
        start_date=datetime.datetime(2022, 1, 1, 0)
    ),
    config_schema={"value": str},
    metadata={"partition_expr": "partition"},
)
def hourly_partitioned(context: AssetExecutionContext) -> pl.DataFrame:
    partition = datetime.datetime.strptime(context.partition_key, "%Y-%m-%d-%H:%M")
    value = context.op_execution_context.op_config["value"]

    return pl.from_dict({"partition": [partition], "value": [value], "b": [1]})


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
def multi_partitioned(context: AssetExecutionContext) -> pl.DataFrame:
    category, date = context.partition_key.split("|")
    date_parsed = datetime.datetime.strptime(date, "%Y-%m-%d").date()
    value = context.op_execution_context.op_config["value"]

    return pl.from_dict(
        {
            "date_this": [date_parsed],
            "value": [value],
            "b": [1],
            "category_this": [category],
        },
    )


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
        "write_mode": "append",
    },
)
def multi_partitioned_append_mode(context: AssetExecutionContext) -> pl.DataFrame:
    category, date = context.partition_key.split("|")
    date_parsed = datetime.datetime.strptime(date, "%Y-%m-%d").date()
    value = context.op_execution_context.op_config["value"]

    return pl.from_dict(
        {
            "date_this": [date_parsed],
            "value": [value],
            "b": [1],
            "category_this": [category],
        },
    )


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
        "write_mode": "overwrite",
    },
)
def multi_partitioned_overwrite_mode(context: AssetExecutionContext) -> pl.DataFrame:
    category, date = context.partition_key.split("|")
    date_parsed = datetime.datetime.strptime(date, "%Y-%m-%d").date()
    value = context.op_execution_context.op_config["value"]

    return pl.from_dict(
        {
            "date_this": [date_parsed],
            "value": [value],
            "b": [1],
            "category_this": [category],
        },
    )


class TestIcebergIOManager:
    def test_output_input_loading_for_asset_dependencies_with_overwrite(
        self,
        asset_b_df_table_identifier: str,
        asset_b_plus_one_table_identifier: str,
        catalog: Catalog,
        io_manager: PolarsIcebergIOManager,
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

    def test_daily_partitioned_assets(
        self,
        asset_daily_partitioned_table_identifier: str,
        catalog: Catalog,
        io_manager: PolarsIcebergIOManager,
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
            datetime.date(2022, 1, 3),
            datetime.date(2022, 1, 2),
            datetime.date(2022, 1, 1),
        ]

    def test_daily_partitioned_assets_overwrite_mode(
        self,
        asset_daily_partitioned_table_identifier: str,
        catalog: Catalog,
        io_manager: PolarsIcebergIOManager,
    ):
        resource_defs = {"io_manager": io_manager}

        for date, value in [
            ("2022-01-01", "1"),
            ("2022-01-01", "2"),
            ("2022-01-01", "3"),
        ]:
            res = materialize(
                [daily_partitioned],
                partition_key=date,
                resources=resource_defs,
                run_config={
                    "ops": {
                        "my_schema__daily_partitioned": {"config": {"value": value}}
                    },
                },
            )
            assert res.success

        table = catalog.load_table(asset_daily_partitioned_table_identifier)
        assert len(table.spec().fields) == 1
        assert table.spec().fields[0].name == "partition"

        out_df = table.scan().to_arrow()
        assert len(out_df) == 1
        assert out_df["partition"].to_pylist() == [
            datetime.date(2022, 1, 1),
        ]
        assert out_df["value"].to_pylist() == ["3"]

    def test_daily_partitioned_assets_append_mode(
        self,
        asset_daily_partitioned_append_mode_table_identifier: str,
        catalog: Catalog,
        io_manager: PolarsIcebergIOManager,
    ):
        resource_defs = {"io_manager": io_manager}

        for date in ["2022-01-01", "2022-01-01", "2022-01-02"]:
            res = materialize(
                [daily_partitioned_append_mode],
                partition_key=date,
                resources=resource_defs,
                run_config={
                    "ops": {
                        "my_schema__daily_partitioned_append_mode": {
                            "config": {"value": "1"}
                        }
                    },
                },
            )
            assert res.success

        table = catalog.load_table(asset_daily_partitioned_append_mode_table_identifier)
        assert len(table.spec().fields) == 1
        assert table.spec().fields[0].name == "partition"

        out_df = table.scan().to_arrow()
        assert sorted(out_df["partition"].to_pylist()) == [
            datetime.date(2022, 1, 1),
            datetime.date(2022, 1, 1),
            datetime.date(2022, 1, 2),
        ]
        assert out_df["value"].to_pylist() == ["1", "1", "1"]

    def test_hourly_partitioned_assets(
        self,
        asset_hourly_partitioned_table_identifier: str,
        catalog: Catalog,
        io_manager: PolarsIcebergIOManager,
    ):
        resource_defs = {"io_manager": io_manager}

        for date in ["2022-01-01-01:00", "2022-01-01-02:00", "2022-01-01-03:00"]:
            res = materialize(
                [hourly_partitioned],
                partition_key=date,
                resources=resource_defs,
                run_config={
                    "ops": {
                        "my_schema__hourly_partitioned": {"config": {"value": "1"}}
                    },
                },
            )
            assert res.success

        table = catalog.load_table(asset_hourly_partitioned_table_identifier)
        assert len(table.spec().fields) == 1
        assert table.spec().fields[0].name == "partition"

        out_df = table.scan().to_arrow()
        assert out_df["partition"].to_pylist() == [
            datetime.datetime(2022, 1, 1, 3, 0),
            datetime.datetime(2022, 1, 1, 2, 0),
            datetime.datetime(2022, 1, 1, 1, 0),
        ]

    def test_multipartitioned_assets(
        self,
        asset_multi_partitioned_table_identifier: str,
        catalog: Catalog,
        io_manager: PolarsIcebergIOManager,
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
            datetime.date(2022, 1, 2),
            datetime.date(2022, 1, 2),
            datetime.date(2022, 1, 2),
            datetime.date(2022, 1, 1),
            datetime.date(2022, 1, 1),
            datetime.date(2022, 1, 1),
        ]
        assert out_df["category_this"].to_pylist() == ["c", "b", "a", "c", "b", "a"]

    def test_multipartitioned_assets_append_mode(
        self,
        asset_multi_partitioned_append_mode_table_identifier: str,
        catalog: Catalog,
        io_manager: PolarsIcebergIOManager,
    ):
        resource_defs = {"io_manager": io_manager}
        keys = [
            "a|2022-01-01",
            "b|2022-01-01",
            "c|2022-01-01",
            "a|2022-01-01",
            "b|2022-01-01",
            "c|2022-01-01",
        ]
        for key in keys:
            res = materialize(
                [multi_partitioned_append_mode],
                partition_key=key,
                resources=resource_defs,
                run_config={
                    "ops": {
                        "my_schema__multi_partitioned_append_mode": {
                            "config": {"value": "1"}
                        }
                    },
                },
            )
            assert res.success

        table = catalog.load_table(asset_multi_partitioned_append_mode_table_identifier)
        assert len(table.spec().fields) == 2
        assert [f.name for f in table.spec().fields] == ["category_this", "date_this"]

        out_df = table.scan().to_arrow()
        assert len(out_df) == len(keys)

        date_this_values = set(out_df["date_this"].to_pylist())
        assert len(date_this_values) == 1
        expected_date_this_values = {
            datetime.datetime.strptime(k.split("|")[1], "%Y-%m-%d").date() for k in keys
        }
        assert date_this_values == expected_date_this_values

        category_this_values = set(out_df["category_this"].to_pylist())
        assert len(category_this_values) == 3
        expected_category_this_values = {k.split("|")[0] for k in keys}
        assert category_this_values == expected_category_this_values

    def test_multipartitioned_assets_overwrite_mode(
        self,
        asset_multi_partitioned_overwrite_mode_table_identifier: str,
        catalog: Catalog,
        io_manager: PolarsIcebergIOManager,
    ):
        resource_defs = {"io_manager": io_manager}
        keys = [
            "a|2022-01-01",
            "b|2022-01-01",
            "c|2022-01-01",
            "a|2022-01-01",
            "b|2022-01-01",
            "c|2022-01-01",
        ]
        for key in keys:
            res = materialize(
                [multi_partitioned_overwrite_mode],
                partition_key=key,
                resources=resource_defs,
                run_config={
                    "ops": {
                        "my_schema__multi_partitioned_overwrite_mode": {
                            "config": {"value": "1"}
                        }
                    },
                },
            )
            assert res.success

        table = catalog.load_table(
            asset_multi_partitioned_overwrite_mode_table_identifier
        )
        assert len(table.spec().fields) == 2
        assert [f.name for f in table.spec().fields] == ["category_this", "date_this"]

        out_df = table.scan().to_arrow()
        assert len(out_df) == 3

        date_this_values = set(out_df["date_this"].to_pylist())
        assert len(date_this_values) == 1
        expected_date_this_values = {
            datetime.datetime.strptime(k.split("|")[1], "%Y-%m-%d").date() for k in keys
        }
        assert date_this_values == expected_date_this_values

        category_this_values = set(out_df["category_this"].to_pylist())
        assert len(category_this_values) == 3
        expected_category_this_values = {k.split("|")[0] for k in keys}
        assert category_this_values == expected_category_this_values

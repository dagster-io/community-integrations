from datetime import datetime

import polars as pl
import polars.testing as pl_testing
import pytest
import pytest_mock
from dagster import (
    AssetExecutionContext,
    AssetIn,
    Config,
    DagsterInstance,
    DailyPartitionsDefinition,
    InputContext,
    MultiPartitionKey,
    MultiPartitionsDefinition,
    OpExecutionContext,
    OutputContext,
    RunConfig,
    StaticPartitionsDefinition,
    TimeWindowPartitionsDefinition,
    asset,
    materialize,
)
from deltalake import DeltaTable  # noqa: TID253

from dagster_polars import PolarsDeltaIOManager
from dagster_polars.io_managers.delta import DeltaSchemaMode, DeltaWriteMode
from dagster_polars_tests.utils import get_saved_path


def test_polars_delta_io_manager_append(polars_delta_io_manager: PolarsDeltaIOManager):
    df = pl.DataFrame(
        {
            "a": [1, 2, 3],
        }
    )

    @asset(io_manager_def=polars_delta_io_manager, metadata={"mode": "append"})
    def append_asset() -> pl.DataFrame:
        return df

    result = materialize(
        [append_asset],
    )

    handled_output_events = list(
        filter(
            lambda evt: evt.is_handled_output, result.events_for_node("append_asset")
        )
    )
    saved_path = handled_output_events[0].event_specific_data.metadata["path"].value  # type: ignore
    assert (
        handled_output_events[0].event_specific_data.metadata["dagster/row_count"].value
        == 3
    )  # type: ignore
    assert (
        handled_output_events[0].event_specific_data.metadata["append_row_count"].value
        == 3
    )  # type: ignore
    assert isinstance(saved_path, str)

    result = materialize(
        [append_asset],
    )
    handled_output_events = list(
        filter(
            lambda evt: evt.is_handled_output, result.events_for_node("append_asset")
        )
    )
    assert (
        handled_output_events[0].event_specific_data.metadata["dagster/row_count"].value
        == 6
    )  # type: ignore
    assert (
        handled_output_events[0].event_specific_data.metadata["append_row_count"].value
        == 3
    )  # type: ignore

    pl_testing.assert_frame_equal(pl.concat([df, df]), pl.read_delta(saved_path))


def test_polars_delta_io_manager_append_lazy(
    polars_delta_io_manager: PolarsDeltaIOManager,
):
    df = pl.DataFrame(
        {
            "a": [1, 2, 3],
        }
    )

    @asset(io_manager_def=polars_delta_io_manager, metadata={"mode": "append"})
    def append_asset() -> pl.LazyFrame:
        return df.lazy()

    result = materialize(
        [append_asset],
    )

    handled_output_events = list(
        filter(
            lambda evt: evt.is_handled_output, result.events_for_node("append_asset")
        )
    )
    saved_path = handled_output_events[0].event_specific_data.metadata["path"].value  # type: ignore
    assert (
        handled_output_events[0].event_specific_data.metadata["dagster/row_count"].value
        == 3
    )  # type: ignore
    assert isinstance(saved_path, str)

    result = materialize(
        [append_asset],
    )
    handled_output_events = list(
        filter(
            lambda evt: evt.is_handled_output, result.events_for_node("append_asset")
        )
    )
    assert (
        handled_output_events[0].event_specific_data.metadata["dagster/row_count"].value
        == 6
    )  # type: ignore

    pl_testing.assert_frame_equal(pl.concat([df, df]), pl.read_delta(saved_path))


def test_polars_delta_io_manager_overwrite_schema(
    polars_delta_io_manager: PolarsDeltaIOManager, dagster_instance: DagsterInstance
):
    @asset(io_manager_def=polars_delta_io_manager)
    def overwrite_schema_asset_1() -> pl.DataFrame:
        return pl.DataFrame(
            {
                "a": [1, 2, 3],
            }
        )

    result = materialize(
        [overwrite_schema_asset_1],
    )

    saved_path = get_saved_path(result, "overwrite_schema_asset_1")

    pl_testing.assert_frame_equal(
        pl.DataFrame(
            {
                "a": [1, 2, 3],
            }
        ),
        pl.read_delta(saved_path),
    )

    @asset(
        io_manager_def=polars_delta_io_manager,
        metadata={
            "delta_write_options": {"schema_mode": "overwrite"},
            "mode": "overwrite",
        },
    )
    def overwrite_schema_asset_2() -> pl.DataFrame:
        return pl.DataFrame(
            {
                "b": ["1", "2", "3"],
            }
        )

    result = materialize(
        [overwrite_schema_asset_2],
    )

    saved_path = get_saved_path(result, "overwrite_schema_asset_2")

    pl_testing.assert_frame_equal(
        pl.DataFrame(
            {
                "b": ["1", "2", "3"],
            }
        ),
        pl.read_delta(saved_path),
    )

    # test IOManager configuration works too
    @asset(
        io_manager_def=PolarsDeltaIOManager(
            base_dir=dagster_instance.storage_directory(),
            mode=DeltaWriteMode.overwrite,
        ),
        metadata={"delta_write_options": {"schema_mode": "overwrite"}},
    )
    def overwrite_schema_asset_3() -> pl.DataFrame:
        return pl.DataFrame(
            {
                "a": [1, 2, 3],
            }
        )

    result = materialize(
        [overwrite_schema_asset_3],
    )

    saved_path = get_saved_path(result, "overwrite_schema_asset_3")

    pl_testing.assert_frame_equal(
        pl.DataFrame(
            {
                "a": [1, 2, 3],
            }
        ),
        pl.read_delta(saved_path),
    )


def test_polars_delta_io_manager_overwrite_schema_lazy(
    polars_delta_io_manager: PolarsDeltaIOManager, dagster_instance: DagsterInstance
):
    @asset(io_manager_def=polars_delta_io_manager)
    def overwrite_schema_asset_1() -> pl.LazyFrame:
        return pl.LazyFrame(
            {
                "a": [1, 2, 3],
            }
        )

    result = materialize(
        [overwrite_schema_asset_1],
    )

    saved_path = get_saved_path(result, "overwrite_schema_asset_1")

    pl_testing.assert_frame_equal(
        pl.DataFrame(
            {
                "a": [1, 2, 3],
            }
        ),
        pl.read_delta(saved_path),
    )

    @asset(
        io_manager_def=polars_delta_io_manager,
        metadata={
            "delta_write_options": {"schema_mode": "overwrite"},
            "mode": "overwrite",
        },
    )
    def overwrite_schema_asset_2() -> pl.LazyFrame:
        return pl.LazyFrame(
            {
                "b": ["1", "2", "3"],
            }
        )

    result = materialize(
        [overwrite_schema_asset_2],
    )

    saved_path = get_saved_path(result, "overwrite_schema_asset_2")

    pl_testing.assert_frame_equal(
        pl.DataFrame(
            {
                "b": ["1", "2", "3"],
            }
        ),
        pl.read_delta(saved_path),
    )

    # test IOManager configuration works too
    @asset(
        io_manager_def=PolarsDeltaIOManager(
            base_dir=dagster_instance.storage_directory(),
            mode=DeltaWriteMode.overwrite,
        ),
        metadata={"delta_write_options": {"schema_mode": "overwrite"}},
    )
    def overwrite_schema_asset_3() -> pl.LazyFrame:
        return pl.LazyFrame(
            {
                "a": [1, 2, 3],
            }
        )

    result = materialize(
        [overwrite_schema_asset_3],
    )

    saved_path = get_saved_path(result, "overwrite_schema_asset_3")

    pl_testing.assert_frame_equal(
        pl.DataFrame(
            {
                "a": [1, 2, 3],
            }
        ),
        pl.read_delta(saved_path),
    )


def test_polars_delta_native_partitioning(
    polars_delta_io_manager: PolarsDeltaIOManager,
    df_for_delta: pl.DataFrame,
):
    manager = polars_delta_io_manager
    df = df_for_delta

    partitions_def = StaticPartitionsDefinition(["a", "b"])

    @asset(
        io_manager_def=manager,
        partitions_def=partitions_def,
        metadata={
            "partition_by": "partition",
        },
    )
    def upstream_partitioned(context: OpExecutionContext) -> pl.DataFrame:
        return df.with_columns(pl.lit(context.partition_key).alias("partition"))

    @asset(io_manager_def=manager)
    def downstream_load_multiple_partitions_as_single_df(
        upstream_partitioned: pl.DataFrame,
    ) -> None:
        assert set(upstream_partitioned["partition"].unique()) == {"a", "b"}

    for partition_key in ["a", "b"]:
        result = materialize(
            [upstream_partitioned],
            partition_key=partition_key,
        )
        saved_path = get_saved_path(result, "upstream_partitioned")
        assert saved_path.endswith("upstream_partitioned.delta"), (
            saved_path
        )  # DeltaLake should handle partitioning!
        assert DeltaTable(saved_path).metadata().partition_columns == ["partition"]

    materialize(
        [
            upstream_partitioned.to_source_asset(),
            downstream_load_multiple_partitions_as_single_df,
        ],
    )


def test_polars_delta_native_partitioning_time_window_string_column(
    polars_delta_io_manager: PolarsDeltaIOManager,
    df_for_delta: pl.DataFrame,
):
    """Regression test for #330.

    A ``TimeWindowPartitionsDefinition`` (yearly) whose key is stored in a
    string column must produce a string predicate (``year = '2025'``) rather
    than a ``DATE '2025'`` literal, which fails with ``Cannot cast string
    '2025' to value of Date32 type``.
    """
    manager = polars_delta_io_manager
    df = df_for_delta

    partitions_def = TimeWindowPartitionsDefinition(
        start="2024",
        fmt="%Y",
        cron_schedule="@yearly",
        end_offset=1,
    )

    @asset(
        io_manager_def=manager,
        partitions_def=partitions_def,
        metadata={"partition_by": "year"},
    )
    def upstream_partitioned(context: OpExecutionContext) -> pl.DataFrame:
        return df.with_columns(pl.lit(context.partition_key).alias("year"))

    @asset(io_manager_def=manager, partitions_def=partitions_def)
    def downstream_partitioned(
        context: AssetExecutionContext, upstream_partitioned: pl.DataFrame
    ) -> None:
        years = upstream_partitioned["year"].unique().to_list()
        assert years == [context.partition_key]

    for partition_key in ["2024", "2025"]:
        result = materialize(
            [upstream_partitioned, downstream_partitioned],
            partition_key=partition_key,
        )
        saved_path = get_saved_path(result, "upstream_partitioned")
        assert saved_path.endswith("upstream_partitioned.delta"), saved_path
        assert DeltaTable(saved_path).metadata().partition_columns == ["year"]


def test_polars_delta_native_partitioning_datetime_column_predicate(
    polars_delta_io_manager: PolarsDeltaIOManager,
    df_for_delta: pl.DataFrame,
):
    """The overwrite predicate must work for a ``Datetime`` partition column.

    In ``0.27.12`` the predicate emitted a ``DATE '...'`` literal, which fails
    to compare against a ``Datetime`` column (`Invalid comparison operation:
    Timestamp <= ...`). A plain string literal is coerced by DataFusion to the
    column's timestamp type, so overwriting one partition leaves the others
    intact.

    Note: this exercises the write/overwrite predicate path only. Reading back a
    *single* partition of a ``Datetime``-partitioned table is a separate,
    pre-existing delta-rs limitation (the partition value cannot be parsed as
    ``timestamp_ntz``), so the table is read back in full here rather than via a
    partitioned downstream asset.
    """
    manager = polars_delta_io_manager
    df = df_for_delta

    partitions_def = DailyPartitionsDefinition(start_date=datetime(2024, 1, 1))

    @asset(
        io_manager_def=manager,
        partitions_def=partitions_def,
        metadata={"partition_by": "ts"},
    )
    def upstream_partitioned(context: OpExecutionContext) -> pl.DataFrame:
        return df.with_columns(
            pl.lit(context.partition_key)
            .str.strptime(pl.Datetime, "%Y-%m-%d")
            .alias("ts")
        )

    saved_path = None
    for partition_key in ["2024-01-01", "2024-01-02"]:
        result = materialize([upstream_partitioned], partition_key=partition_key)
        saved_path = get_saved_path(result, "upstream_partitioned")

    assert saved_path is not None
    assert DeltaTable(saved_path).metadata().partition_columns == ["ts"]

    # Both partition writes succeeded and neither predicate clobbered the other.
    written = pl.read_delta(saved_path)["ts"].unique().sort().to_list()
    assert written == [datetime(2024, 1, 1), datetime(2024, 1, 2)]


def test_polars_delta_native_multi_partitions(
    polars_delta_io_manager: PolarsDeltaIOManager,
    df_for_delta: pl.DataFrame,
):
    manager = polars_delta_io_manager
    df = df_for_delta

    partitions_def = MultiPartitionsDefinition(
        {
            "time": DailyPartitionsDefinition(start_date=datetime(2024, 1, 1)),
            "category": StaticPartitionsDefinition(["a", "b"]),
        }
    )

    @asset(
        io_manager_def=manager,
        partitions_def=partitions_def,
        metadata={
            "partition_by": {"time": "date", "category": "category"},
        },
    )
    def upstream_partitioned(context: OpExecutionContext) -> pl.DataFrame:
        partition_key = context.partition_key
        assert isinstance(partition_key, MultiPartitionKey)
        return df.with_columns(
            pl.lit(partition_key.keys_by_dimension["time"])
            .str.strptime(pl.Date, "%Y-%m-%d")
            .alias("date"),
            pl.lit(partition_key.keys_by_dimension["category"]).alias("category"),
        )

    @asset(io_manager_def=manager)
    def downstream_load_multiple_partitions_as_single_df(
        upstream_partitioned: pl.DataFrame,
    ) -> None:
        assert set(upstream_partitioned["category"].unique()) == {"a", "b"}
        assert set(upstream_partitioned["date"].unique()) == {
            datetime(2024, 1, 1).date(),
            datetime(2024, 1, 2).date(),
        }

    for date in ["2024-01-01", "2024-01-02"]:
        for category in ["a", "b"]:
            materialize([upstream_partitioned], partition_key=f"{category}|{date}")

    materialize(
        [
            upstream_partitioned.to_source_asset(),
            downstream_load_multiple_partitions_as_single_df,
        ],
    )


def test_polars_delta_native_partitioning_loading_single_partition(
    polars_delta_io_manager: PolarsDeltaIOManager,
    df_for_delta: pl.DataFrame,
):
    manager = polars_delta_io_manager
    df = df_for_delta

    partitions_def = StaticPartitionsDefinition(["a", "b"])

    @asset(
        io_manager_def=manager,
        partitions_def=partitions_def,
        metadata={
            "partition_by": "partition",
        },
    )
    def upstream_partitioned(context: OpExecutionContext) -> pl.DataFrame:
        return df.with_columns(pl.lit(context.partition_key).alias("partition"))

    @asset(io_manager_def=manager, partitions_def=partitions_def)
    def downstream_partitioned(
        context: AssetExecutionContext, upstream_partitioned: pl.DataFrame
    ) -> None:
        partitions = upstream_partitioned["partition"].unique().to_list()
        assert len(partitions) == 1
        assert partitions[0] == context.partition_key

    for partition_key in ["a", "b"]:
        materialize(
            [upstream_partitioned, downstream_partitioned],
            partition_key=partition_key,
        )


def test_polars_delta_time_travel(
    polars_delta_io_manager: PolarsDeltaIOManager, df_for_delta: pl.DataFrame
):
    manager = polars_delta_io_manager
    df = df_for_delta

    class UpstreamConfig(Config):
        foo: str

    @asset(io_manager_def=manager)
    def upstream(context: OpExecutionContext, config: UpstreamConfig) -> pl.DataFrame:
        return df.with_columns(pl.lit(config.foo).alias("foo"))

    for foo in ["a", "b"]:
        materialize(
            [upstream], run_config=RunConfig(ops={"upstream": UpstreamConfig(foo=foo)})
        )

    # get_saved_path(result, "upstream")

    @asset(ins={"upstream": AssetIn(metadata={"version": 0})})
    def downstream_0(upstream: pl.DataFrame) -> None:
        assert upstream["foo"].head(1).item() == "a"

    materialize(
        [
            upstream.to_source_asset(),
            downstream_0,
        ]
    )

    @asset(ins={"upstream": AssetIn(metadata={"version": "1"})})
    def downstream_1(upstream: pl.DataFrame) -> None:
        assert upstream["foo"].head(1).item() == "b"

    materialize(
        [
            upstream.to_source_asset(),
            downstream_1,
        ]
    )


def test_polars_delta_io_manager_schema_mode_set(dagster_instance: DagsterInstance):
    manager = PolarsDeltaIOManager(
        base_dir=dagster_instance.storage_directory(),
        mode=DeltaWriteMode.overwrite,
        schema_mode=DeltaSchemaMode.overwrite,
    )

    @asset(io_manager_def=manager, name="my_asset")
    def asset_schema_1(context: OpExecutionContext) -> pl.DataFrame:
        return pl.DataFrame({"foo": ["a", "b"]})

    res = materialize([asset_schema_1])

    assert pl.scan_delta(get_saved_path(res, "my_asset")).columns == ["foo"]

    @asset(io_manager_def=manager, name="my_asset")
    def asset_schema_2(context: OpExecutionContext) -> pl.DataFrame:
        return pl.DataFrame({"bar": [1, 2, 3]})

    materialize([asset_schema_2])

    assert pl.scan_delta(get_saved_path(res, "my_asset")).columns == ["bar"]


@pytest.mark.parametrize(
    "partition_by, partition_keys, expected_filters, expected_predicate",
    [
        ("col_name", ["a"], [("col_name", "in", ["a"])], "col_name = 'a'"),
        (
            "col_name",
            ["a", "b"],
            [("col_name", "in", ["a", "b"])],
            "col_name = 'a' OR col_name = 'b'",
        ),
        (
            {"col_name": "mapped_col"},
            [{"col_name": "a"}],
            [("mapped_col", "in", ["a"])],
            "(mapped_col = 'a')",
        ),
        (
            {"col_name": "mapped_col"},
            [{"col_name": "a"}, {"col_name": "b"}],
            [("mapped_col", "in", ["a", "b"])],
            "(mapped_col = 'a' OR mapped_col = 'b')",
        ),
        (None, [], [], None),
    ],
)
@pytest.mark.parametrize("context", [InputContext, OutputContext])
def test_partition_filters_predicate(
    mocker: pytest_mock.MockerFixture,
    partition_by: str | dict[str, str] | None,
    partition_keys: list[str] | list[dict[str, str]],
    expected_filters: list[tuple[str, str, list[str]]],
    expected_predicate: str,
    context: type[InputContext | OutputContext],
):
    """Test that the partition filters and predicate are generated correctly."""
    if context == InputContext:
        context = mocker.MagicMock(InputContext)
        mock_upstream_output = mocker.MagicMock(OutputContext)
        type(mock_upstream_output).definition_metadata = mocker.PropertyMock(
            return_value={"partition_by": partition_by}
        )
        type(context).upstream_output = mocker.PropertyMock(
            return_value=mock_upstream_output
        )
    else:
        context = mocker.MagicMock(OutputContext)
        type(context).definition_metadata = mocker.PropertyMock(
            return_value={"partition_by": partition_by}
        )

    type(context).has_asset_partitions = mocker.PropertyMock(return_value=True)

    if len(partition_keys) and isinstance(partition_keys[0], dict):
        mock_keys = []

        for keys in partition_keys:
            mock_partition_keys = mocker.MagicMock(MultiPartitionKey)
            type(mock_partition_keys).keys_by_dimension = mocker.PropertyMock(
                return_value=keys
            )
            mock_keys.append(mock_partition_keys)

        type(context).asset_partition_keys = mocker.PropertyMock(return_value=mock_keys)
    else:
        type(context).asset_partition_keys = mocker.PropertyMock(
            return_value=partition_keys
        )

    assert PolarsDeltaIOManager.get_partition_filters(context) == expected_filters
    assert PolarsDeltaIOManager.get_predicate(context) == expected_predicate

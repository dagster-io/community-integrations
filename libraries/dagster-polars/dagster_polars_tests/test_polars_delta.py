from datetime import datetime
from typing import Optional, Union

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
    asset,
    materialize,
)
from deltalake import DeltaTable  # noqa: TID253

from dagster_polars import PolarsDeltaIOManager
from dagster_polars.io_managers.delta import DeltaWriteMode
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


@pytest.mark.parametrize(
    "partition_by, partition_keys, expected_filters, expected_predicate",
    [
        ("col_name", ["a"], [("col_name", "in", ["a"])], "col_name = 'a'"),
        (
            "col_name",
            ["a", "b"],
            [("col_name", "in", ["a", "b"])],
            "col_name in ('a', 'b')",
        ),
        (
            {"col_name": "mapped_col"},
            [{"col_name": "a"}],
            [("mapped_col", "in", ["a"])],
            "mapped_col in ('a')",
        ),
        (
            {"col_name": "mapped_col"},
            [{"col_name": "a"}, {"col_name": "b"}],
            [("mapped_col", "in", ["a", "b"])],
            "mapped_col in ('a', 'b')",
        ),
        (None, [], [], None),
    ],
)
@pytest.mark.parametrize("context", [InputContext, OutputContext])
def test_partition_filters_predicate(
    mocker: pytest_mock.MockerFixture,
    partition_by: Optional[Union[str, dict[str, str]]],
    partition_keys: Union[list[str], list[dict[str, str]]],
    expected_filters: list[tuple[str, str, list[str]]],
    expected_predicate: str,
    context: type[Union[InputContext, OutputContext]],
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


def test_delta_merge_options_ignored_outside_merge(
    polars_delta_io_manager: PolarsDeltaIOManager,
):
    df = pl.DataFrame({"a": [1, 2], "b": ["a", "b"]})

    @asset(
        io_manager_def=polars_delta_io_manager,
        metadata={
            "mode": "append",  # not 'merge'
            "delta_merge_options": {"predicate": "id == id"},
        },
    )
    def asset_with_merge_options() -> pl.DataFrame:
        return df

    result = materialize([asset_with_merge_options])
    saved_path = get_saved_path(result, "asset_with_merge_options")

    pl_testing.assert_frame_equal(
        df,
        pl.read_delta(saved_path),
    )


def test_merge_data(polars_delta_io_manager: PolarsDeltaIOManager):
    df1 = pl.DataFrame({"id": [1, 2], "val": ["a", "oldB"]})
    df2 = pl.DataFrame({"id": [2, 3], "val": ["newB", "c"]})

    @asset(
        io_manager_def=polars_delta_io_manager,
    )
    def merge_asset() -> pl.DataFrame:
        return df1

    result = materialize([merge_asset])
    saved_path = get_saved_path(result, "merge_asset")

    @asset(
        io_manager_def=polars_delta_io_manager,
        metadata={
            "mode": "merge",
            "delta_merge_options": {
                "source_alias": "s",
                "target_alias": "t",
                "predicate": "s.id == t.id",
            },
        },
    )
    def merge_asset() -> pl.DataFrame:
        return df2

    materialize([merge_asset])
    expected = pl.DataFrame({"id": [1, 2, 3], "val": ["a", "newB", "c"]})
    actual = pl.read_delta(saved_path).sort("id")
    pl_testing.assert_frame_equal(actual, expected.sort("id"))


def test_merge_data_and_schema(polars_delta_io_manager: PolarsDeltaIOManager):
    df1 = pl.DataFrame({"id": [1, 2], "val": ["a", "oldB"]})
    df2 = pl.DataFrame({"id": [2, 3], "val": ["newB", "c"], "extra": [10, 20]})

    @asset(
        io_manager_def=polars_delta_io_manager,
    )
    def merge_schema_asset() -> pl.DataFrame:
        return df1

    result = materialize([merge_schema_asset])
    saved_path = get_saved_path(result, "merge_schema_asset")

    @asset(
        io_manager_def=polars_delta_io_manager,
        metadata={
            "mode": "merge",
            "delta_merge_options": {
                "source_alias": "s",
                "target_alias": "t",
                "predicate": "s.id == t.id",
                "merge_schema": True,
            },
        },
    )
    def merge_schema_asset() -> pl.DataFrame:
        return df2

    materialize([merge_schema_asset])
    expected = pl.DataFrame(
        {"id": [1, 2, 3], "val": ["a", "newB", "c"], "extra": [None, 10, 20]}
    )
    actual = pl.read_delta(saved_path).sort("id")
    pl_testing.assert_frame_equal(actual, expected.sort("id"))


def test_override_default_merge_command(
    polars_delta_io_manager: PolarsDeltaIOManager,
):
    df1 = pl.DataFrame({"id": [1, 2], "val": ["a", "willBeDeletedByMerge"]})
    df2 = pl.DataFrame(
        {"id": [1, 3], "val": ["willNotInsertNewValue", "willNotInsert"]}
    )

    @asset(
        io_manager_def=polars_delta_io_manager,
    )
    def merge_schema_asset() -> pl.DataFrame:
        return df1

    result = materialize([merge_schema_asset])
    saved_path = get_saved_path(result, "merge_schema_asset")

    @asset(
        io_manager_def=polars_delta_io_manager,
        metadata={
            "mode": "merge",
            "delta_merge_options": {
                "source_alias": "s",
                "target_alias": "t",
                "predicate": "s.id == t.id",
            },
            "when_matched_update": {},
            "when_not_matched_insert": {},
            "when_not_matched_by_source_delete": "all",
        },
    )
    def merge_schema_asset() -> pl.DataFrame:
        return df2

    materialize([merge_schema_asset])
    expected = pl.DataFrame({"id": [1], "val": ["a"]})
    actual = pl.read_delta(saved_path).sort("id")
    pl_testing.assert_frame_equal(actual, expected.sort("id"))


def test_polars_delta_merge_with_partitioning(
    polars_delta_io_manager: PolarsDeltaIOManager,
):
    df1 = pl.DataFrame({"id": [1, 2], "val": ["a", "b"], "partition": ["x", "y"]})
    df2 = pl.DataFrame({"id": [2, 3], "val": ["newB", "c"], "partition": ["y", "x"]})

    partitions_def = StaticPartitionsDefinition(["x", "y"])

    @asset(
        io_manager_def=polars_delta_io_manager,
        partitions_def=partitions_def,
        metadata={"partition_by": "partition"},
    )
    def merge_partitioned(context: OpExecutionContext) -> pl.DataFrame:
        return df1.filter(pl.col("partition") == context.partition_key)

    for partition_key in ["x", "y"]:
        materialize([merge_partitioned], partition_key=partition_key)

    @asset(
        io_manager_def=polars_delta_io_manager,
        metadata={
            "mode": "merge",
            "delta_merge_options": {
                "source_alias": "s",
                "target_alias": "t",
                "predicate": "s.id == t.id and s.partition == t.partition",
            },
        },
    )
    def merge_partitioned() -> pl.DataFrame:
        return df2

    result = materialize([merge_partitioned])
    saved_path = get_saved_path(result, "merge_partitioned")

    # After merge, both partitions should exist and be correct
    merged = pl.read_delta(saved_path).sort("id")
    expected = pl.DataFrame(
        {"id": [1, 2, 3], "val": ["a", "newB", "c"], "partition": ["x", "y", "x"]}
    ).sort("id")
    pl_testing.assert_frame_equal(merged, expected)


def test_granular_merge_selectors_and_predicates(
    polars_delta_io_manager: PolarsDeltaIOManager,
):
    df1 = pl.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]})
    df2 = pl.DataFrame({"id": [2, 3, 4], "val": ["newB", "c", "d"]})

    @asset(io_manager_def=polars_delta_io_manager)
    def merge_asset() -> pl.DataFrame:
        return df1

    result = materialize([merge_asset])
    saved_path = get_saved_path(result, "merge_asset")

    # Only update row with id=2, insert only id=4, delete id=3
    @asset(
        io_manager_def=polars_delta_io_manager,
        metadata={
            "mode": "merge",
            "delta_merge_options": {
                "source_alias": "s",
                "target_alias": "t",
                "predicate": "s.id == t.id",
            },
            "when_not_matched_insert": {
                "updates": {"id": "s.id", "val": "s.val"},
                "predicate": "s.id == 4",
            },
            "when_matched_update": {
                "updates": {"val": "s.val"},
                "predicate": "t.id == 2",
            },
            "when_matched_delete": {"predicate": "t.id == 3"},
        },
    )
    def merge_asset() -> pl.DataFrame:
        return df2

    materialize([merge_asset])
    expected = pl.DataFrame({"id": [1, 2, 4], "val": ["a", "newB", "d"]})
    actual = pl.read_delta(saved_path).sort("id")
    pl_testing.assert_frame_equal(actual, expected.sort("id"))


def test_granular_merge_selectors_and_predicates_partitioned(
    polars_delta_io_manager: PolarsDeltaIOManager,
):
    df1 = pl.DataFrame({"id": [1, 2], "val": ["a", "b"], "partition": ["x", "y"]})
    df2 = pl.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "val": [None, "newB", "c", "notMerged"],
            "partition": ["x", "y", "x", "y"],
        }
    )
    partitions_def = StaticPartitionsDefinition(["x", "y"])

    @asset(
        io_manager_def=polars_delta_io_manager,
        partitions_def=partitions_def,
        metadata={"partition_by": "partition"},
    )
    def merge_partitioned(context: OpExecutionContext) -> pl.DataFrame:
        return df1.filter(pl.col("partition") == context.partition_key)

    for partition_key in ["x", "y"]:
        result = materialize([merge_partitioned], partition_key=partition_key)
    saved_path = get_saved_path(result, "merge_partitioned")

    # Only update id=2 in partition y, insert id=3 in partition x, delete id=1 in partition x
    @asset(
        io_manager_def=polars_delta_io_manager,
        metadata={
            "mode": "merge",
            "delta_merge_options": {
                "source_alias": "s",
                "target_alias": "t",
                "predicate": "s.id == t.id and s.partition == t.partition",
            },
            "when_not_matched_insert": {
                "updates": {"id": "s.id", "val": "s.val", "partition": "s.partition"},
                "predicate": "s.id == 3",
            },
            "when_matched_update": {
                "updates": {"val": "s.val"},
                "predicate": "t.id == 2",
            },
            "when_matched_delete": {"predicate": "t.id == 1"},
        },
    )
    def merge_partitioned() -> pl.DataFrame:
        return df2

    materialize([merge_partitioned])
    merged = pl.read_delta(saved_path).sort("id")
    expected = pl.DataFrame(
        {"id": [2, 3], "val": ["newB", "c"], "partition": ["y", "x"]}
    ).sort("id")
    pl_testing.assert_frame_equal(merged, expected)

"""Polars, Daft, and PyIceberg load exactly the rows inside a time window partition."""

import datetime as dt
from collections.abc import Callable

import daft
import polars as pl
import pyarrow as pa
import pytest
from dagster import (
    DailyPartitionsDefinition,
    HourlyPartitionsDefinition,
    TimeWindow,
)
from dagster._core.storage.db_io_manager import TablePartitionDimension, TableSlice
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform, HourTransform, IdentityTransform
from pyiceberg.types import (
    DateType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
    TimestamptzType,
)

from dagster_iceberg._utils.io import get_expression_row_filter, table_writer
from dagster_iceberg.io_manager.arrow import _PyArrowIcebergTypeHandler
from dagster_iceberg.io_manager.daft import _DaftIcebergTypeHandler
from dagster_iceberg.io_manager.polars import _PolarsIcebergTypeHandler

CATEGORIES = ["A", "B", "C"]
# Every 15 minutes across the 2024 US spring-forward and fall-back days.
TIMES = [
    start + dt.timedelta(minutes=15 * i)
    for start in (dt.datetime(2024, 3, 8), dt.datetime(2024, 11, 2))
    for i in range(4 * 24 * 4)
]

SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=False),
    NestedField(2, "ts", TimestampType(), required=False),
    NestedField(3, "d", DateType(), required=False),
    NestedField(4, "category", StringType(), required=False),
    NestedField(5, "tstz", TimestamptzType(), required=False),
)


def partitioned_table(catalog: SqlCatalog, column_id: int, transform) -> Table:
    table = catalog.create_table(
        f"ns.by_{column_id}",
        schema=SCHEMA,
        partition_spec=PartitionSpec(
            PartitionField(
                source_id=column_id, field_id=1000, transform=transform, name="p_time"
            ),
            PartitionField(
                source_id=4, field_id=1001, transform=IdentityTransform(), name="p_cat"
            ),
        ),
    )
    table.append(
        pa.table(
            {
                "id": pa.array(range(len(TIMES)), pa.int64()),
                "ts": pa.array(TIMES, pa.timestamp("us")),
                "d": pa.array([t.date() for t in TIMES], pa.date32()),
                "category": [CATEGORIES[i % 3] for i in range(len(TIMES))],
                "tstz": pa.array(
                    [t.replace(tzinfo=dt.UTC) for t in TIMES],
                    pa.timestamp("us", tz="UTC"),
                ),
            }
        )
    )
    return table


@pytest.fixture(scope="module")
def catalog(tmp_path_factory: pytest.TempPathFactory) -> SqlCatalog:
    tmp_path = tmp_path_factory.mktemp("catalog")
    catalog = SqlCatalog(
        "test",
        uri=f"sqlite:///{tmp_path / 'catalog.db'}",
        warehouse=f"file://{tmp_path / 'warehouse'}",
    )
    catalog.create_namespace("ns")
    return catalog


@pytest.fixture(scope="module")
def timestamp_table(catalog: SqlCatalog) -> Table:
    return partitioned_table(catalog, 2, HourTransform())


@pytest.fixture(scope="module")
def timestamptz_table(catalog: SqlCatalog) -> Table:
    return partitioned_table(catalog, 5, HourTransform())


@pytest.fixture(scope="module")
def date_table(catalog: SqlCatalog) -> Table:
    return partitioned_table(catalog, 3, DayTransform())


Loader = Callable[[Table, TableSlice], list[int]]


def load_with_daft(table: Table, table_slice: TableSlice) -> list[int]:
    frame = _DaftIcebergTypeHandler().to_data_frame(table, table_slice, daft.DataFrame)
    return sorted(frame.to_pydict()["id"])


def polars_loader(reader_override) -> Loader:
    def load(table: Table, table_slice: TableSlice) -> list[int]:
        frame = _PolarsIcebergTypeHandler().to_data_frame(
            table, table_slice, pl.DataFrame, reader_override=reader_override
        )
        return sorted(frame["id"].to_list())

    return load


def load_with_pyiceberg(table: Table, table_slice: TableSlice) -> list[int]:
    frame = _PyArrowIcebergTypeHandler().to_data_frame(table, table_slice, pa.Table)
    return sorted(frame["id"].to_pylist())


LOADERS = {
    "daft": load_with_daft,
    "pyiceberg": load_with_pyiceberg,
    "polars": polars_loader(None),
    "polars-pyiceberg": polars_loader("pyiceberg"),
}


@pytest.fixture(params=list(LOADERS))
def load(request: pytest.FixtureRequest) -> Loader:
    return LOADERS[request.param]


UTC_DAILY = DailyPartitionsDefinition(start_date="2024-01-01")
NY_DAILY = DailyPartitionsDefinition(
    start_date="2024-01-01", timezone="America/New_York"
)
UTC_HOURLY = HourlyPartitionsDefinition(start_date="2024-01-01-00:00")
NY_HOURLY = HourlyPartitionsDefinition(
    start_date="2024-01-01-00:00", timezone="America/New_York"
)

DAILY_WINDOWS = {
    "utc day": UTC_DAILY.time_window_for_partition_key("2024-03-09"),
    "new york spring-forward day": NY_DAILY.time_window_for_partition_key("2024-03-10"),
    "new york fall-back day": NY_DAILY.time_window_for_partition_key("2024-11-03"),
    "three days": TimeWindow(
        dt.datetime(2024, 3, 8, tzinfo=dt.UTC), dt.datetime(2024, 3, 11, tzinfo=dt.UTC)
    ),
}
HOURLY_WINDOWS = {
    "utc hour": UTC_HOURLY.time_window_for_partition_key("2024-03-09-05:00"),
    "new york spring-forward hour": NY_HOURLY.time_window_for_partition_key(
        "2024-03-10-01:00"
    ),
    "new york hour before fall-back": NY_HOURLY.time_window_for_partition_key(
        "2024-11-03-00:00"
    ),
    "new york hour after fall-back": NY_HOURLY.time_window_for_partition_key(
        "2024-11-03-02:00"
    ),
    "across midnight": TimeWindow(
        dt.datetime(2024, 3, 9, 22, tzinfo=dt.UTC),
        dt.datetime(2024, 3, 10, 2, tzinfo=dt.UTC),
    ),
    **DAILY_WINDOWS,
}
# Both 1 a.m. hours of the New York fall-back day have the same wall-clock time.
REPEATED_HOURS = {
    "first new york 1am on fall-back": NY_HOURLY.time_window_for_partition_key(
        "2024-11-03-01:00"
    ),
    "second new york 1am on fall-back": NY_HOURLY.time_window_for_partition_key(
        "2024-11-03-01:00-0500"
    ),
}
CATEGORY_FILTERS = {"no category": None, "one category": ["A"], "two": ["A", "C"]}


def window_slice(
    column: str, window: TimeWindow, categories: list[str] | None
) -> TableSlice:
    dimensions = [TablePartitionDimension(partition_expr=column, partitions=window)]
    if categories is not None:
        dimensions.append(
            TablePartitionDimension(partition_expr="category", partitions=categories)
        )
    return TableSlice(table="t", schema="ns", partition_dimensions=dimensions)


def expected_ids(
    value: Callable[[dt.datetime], dt.date | dt.datetime],
    window: TimeWindow,
    categories: list[str] | None,
) -> list[int]:
    # A window selects by wall-clock time in its own timezone.
    start = value(window.start.replace(tzinfo=None))
    end = value(window.end.replace(tzinfo=None))
    return [
        i
        for i, t in enumerate(TIMES)
        if start <= value(t) < end
        and (categories is None or CATEGORIES[i % 3] in categories)
    ]


@pytest.mark.parametrize(
    "categories", CATEGORY_FILTERS.values(), ids=list(CATEGORY_FILTERS)
)
@pytest.mark.parametrize("window", HOURLY_WINDOWS.values(), ids=list(HOURLY_WINDOWS))
def test_timestamp_window_loads_rows_inside_it(
    timestamp_table: Table,
    load: Loader,
    window: TimeWindow,
    categories: list[str] | None,
):
    loaded = load(timestamp_table, window_slice("ts", window, categories))
    assert loaded == expected_ids(lambda t: t, window, categories)


@pytest.mark.parametrize(
    "categories", CATEGORY_FILTERS.values(), ids=list(CATEGORY_FILTERS)
)
@pytest.mark.parametrize("window", DAILY_WINDOWS.values(), ids=list(DAILY_WINDOWS))
def test_date_window_loads_rows_inside_it(
    date_table: Table,
    load: Loader,
    window: TimeWindow,
    categories: list[str] | None,
):
    loaded = load(date_table, window_slice("d", window, categories))
    assert loaded == expected_ids(lambda t: t.date(), window, categories)


def expected_instant_ids(window: TimeWindow, categories: list[str] | None) -> list[int]:
    return [
        i
        for i, t in enumerate(TIMES)
        if window.start <= t.replace(tzinfo=dt.UTC) < window.end
        and (categories is None or CATEGORIES[i % 3] in categories)
    ]


@pytest.mark.parametrize(
    "categories", CATEGORY_FILTERS.values(), ids=list(CATEGORY_FILTERS)
)
@pytest.mark.parametrize(
    "window",
    [*HOURLY_WINDOWS.values(), *REPEATED_HOURS.values()],
    ids=[*HOURLY_WINDOWS, *REPEATED_HOURS],
)
def test_timestamptz_window_loads_rows_inside_it(
    timestamptz_table: Table,
    load: Loader,
    window: TimeWindow,
    categories: list[str] | None,
):
    loaded = load(timestamptz_table, window_slice("tstz", window, categories))
    assert loaded == expected_instant_ids(window, categories)


def test_timestamp_window_that_repeats_a_wall_clock_hour_is_rejected_on_load(
    timestamp_table: Table, load: Loader
):
    window = REPEATED_HOURS["first new york 1am on fall-back"]
    with pytest.raises(ValueError, match=r"2024-11-03T01:00:00-04:00.*repeated"):
        load(timestamp_table, window_slice("ts", window, None))


def test_timestamp_window_that_repeats_a_wall_clock_hour_is_rejected_on_write(
    timestamp_table: Table,
):
    window = REPEATED_HOURS["first new york 1am on fall-back"]
    with pytest.raises(ValueError, match=r"2024-11-03T01:00:00-04:00.*repeated"):
        get_expression_row_filter(
            iceberg_table_schema=timestamp_table.schema(),
            iceberg_partition_spec=timestamp_table.spec(),
            dagster_partition_dimensions=window_slice(
                "ts", window, None
            ).partition_dimensions,
        )


@pytest.mark.parametrize("key", ["2024-11-03-01:00", "2024-11-03-01:00-0500"])
def test_rewriting_a_timestamptz_partition_replaces_its_rows(
    catalog: SqlCatalog, key: str
):
    window = NY_HOURLY.time_window_for_partition_key(key)
    # Adding to a zoned datetime is wall-clock arithmetic, so step through UTC.
    start = window.start.astimezone(dt.UTC)
    table_slice = TableSlice(
        table="rewrite_" + key.replace("-", "_").replace(":", ""),
        schema="ns",
        partition_dimensions=[
            TablePartitionDimension(partition_expr="tstz", partitions=window)
        ],
    )
    data = pa.table(
        {
            "id": pa.array([1, 2], pa.int64()),
            "tstz": pa.array(
                [start, start + dt.timedelta(minutes=30)],
                pa.timestamp("us", tz="UTC"),
            ),
        }
    )
    for _ in range(2):
        table_writer(
            table_slice=table_slice,
            data=data,
            catalog=catalog,
            schema_update_mode="update",
            partition_spec_update_mode="update",
            dagster_run_id="run",
            dagster_partition_key=key,
        )
    table = catalog.load_table(f"ns.{table_slice.table}")
    assert sorted(table.scan().to_arrow()["id"].to_pylist()) == [1, 2]

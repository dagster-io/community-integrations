"""Polars and Daft read `*` and `^...$` as literal column names, not patterns."""

import datetime as dt
from pathlib import Path

import daft
import polars as pl
import pyarrow as pa
import pytest
from dagster import TimeWindow
from dagster._core.storage.db_io_manager import TablePartitionDimension, TableSlice
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import LongType, NestedField, StringType, TimestampType

from dagster_iceberg.io_manager.daft import (
    DagsterPartitionToDaftExpressionMapper,
    _DaftIcebergTypeHandler,
)
from dagster_iceberg.io_manager.polars import (
    DagsterPartitionToPolarsExpressionMapper,
    _PolarsIcebergTypeHandler,
)

JAN_1 = dt.datetime(2024, 1, 1)
JAN_2 = dt.datetime(2024, 1, 2)

# "kind" and "when" match the patterns "^k.*$" and "^w.*$" but hold other values,
# so a filter that reads a name as a pattern selects the wrong rows.
ROWS = pa.table(
    {
        "id": [0, 1, 2, 3],
        "*": ["east", "west", "east", "west"],
        "^k.*$": ["east", "west", "east", "west"],
        "kind": ["west", "east", "west", "east"],
        "^w.*$": [JAN_1, JAN_1, JAN_2, JAN_2],
        "when": [JAN_2, JAN_2, JAN_1, JAN_1],
    }
)
ROWS_SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=False),
    NestedField(2, "*", StringType(), required=False),
    NestedField(3, "^k.*$", StringType(), required=False),
    NestedField(4, "kind", StringType(), required=False),
    NestedField(5, "^w.*$", TimestampType(), required=False),
    NestedField(6, "when", TimestampType(), required=False),
)
JAN_1_WINDOW = TimeWindow(JAN_1.replace(tzinfo=dt.UTC), JAN_2.replace(tzinfo=dt.UTC))
DIMENSIONS = {
    "* key": (TablePartitionDimension(partition_expr="*", partitions=["east"]), [0, 2]),
    "regex key": (
        TablePartitionDimension(partition_expr="^k.*$", partitions=["east"]),
        [0, 2],
    ),
    "regex time window": (
        TablePartitionDimension(partition_expr="^w.*$", partitions=JAN_1_WINDOW),
        [0, 1],
    ),
}


@pytest.mark.parametrize(
    ("dimension", "expected"), DIMENSIONS.values(), ids=list(DIMENSIONS)
)
def test_polars_partition_filter_reads_the_named_column(
    dimension: TablePartitionDimension, expected: list[int]
):
    predicates = DagsterPartitionToPolarsExpressionMapper(
        partition_dimensions=[dimension], table_schema=ROWS_SCHEMA
    ).partition_dimensions_to_filters()
    frame = pl.from_arrow(ROWS).lazy().filter(*predicates).collect()
    assert sorted(frame["id"].to_list()) == expected


# daft.col reads "*" as every column; the Daft I/O manager handles that name while
# it reads a table, as the table tests below show.
@pytest.mark.parametrize(
    ("dimension", "expected"),
    [DIMENSIONS["regex key"], DIMENSIONS["regex time window"]],
    ids=["regex key", "regex time window"],
)
def test_daft_partition_filter_reads_the_named_column(
    dimension: TablePartitionDimension, expected: list[int]
):
    predicates = DagsterPartitionToDaftExpressionMapper(
        partition_dimensions=[dimension], table_schema=ROWS_SCHEMA
    ).partition_dimensions_to_filters()
    frame = daft.from_arrow(ROWS)
    for predicate in predicates:
        frame = frame.where(predicate)
    assert sorted(frame.to_pydict()["id"]) == expected


# "plain" and "pick" match the pattern "^p.*$".
COLUMNS = ["*", "^p.*$", "plain", "pick"]
REGIONS = ["east", "west"]


@pytest.fixture
def patterns_table(tmp_path: Path) -> Table:
    catalog = SqlCatalog(
        "test",
        uri=f"sqlite:///{tmp_path / 'catalog.db'}",
        warehouse=f"file://{tmp_path / 'warehouse'}",
    )
    catalog.create_namespace("ns")
    table = catalog.create_table(
        "ns.patterns",
        schema=Schema(
            *(
                NestedField(field_id, name, StringType(), required=False)
                for field_id, name in enumerate(COLUMNS, start=1)
            )
        ),
        partition_spec=PartitionSpec(
            PartitionField(
                source_id=1, field_id=1000, transform=IdentityTransform(), name="*"
            )
        ),
    )
    table.append(
        pa.table({name: [f"{name}/{region}" for region in REGIONS] for name in COLUMNS})
    )
    return table


def east_slice(columns: list[str]) -> TableSlice:
    return TableSlice(
        table="patterns",
        schema="ns",
        columns=columns,
        partition_dimensions=[
            TablePartitionDimension(partition_expr="*", partitions=["*/east"])
        ],
    )


@pytest.mark.parametrize("columns", [["*"], ["^p.*$"], ["^p.*$", "*", "pick"]])
def test_polars_selects_the_named_columns(patterns_table: Table, columns: list[str]):
    frame = _PolarsIcebergTypeHandler().to_data_frame(
        patterns_table, east_slice(columns), pl.DataFrame
    )
    assert frame.to_dict(as_series=False) == {
        column: [f"{column}/east"] for column in columns
    }


def test_polars_raises_for_a_missing_column(patterns_table: Table):
    with pytest.raises(pl.exceptions.ColumnNotFoundError, match="missing"):
        _PolarsIcebergTypeHandler().to_data_frame(
            patterns_table, east_slice(["missing"]), pl.DataFrame
        )


# Daft can only read "*" here from the partition values; see test_column_names.py.
def test_daft_selects_the_named_column(patterns_table: Table):
    frame = _DaftIcebergTypeHandler().to_data_frame(
        patterns_table, east_slice(["*"]), daft.DataFrame
    )
    assert frame.to_pydict() == {"*": ["*/east"]}

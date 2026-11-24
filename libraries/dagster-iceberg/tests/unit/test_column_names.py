"""Polars and Daft select and filter columns by their exact names."""

from pathlib import Path

import daft
import polars as pl
import pyarrow as pa
import pytest
from dagster._core.storage.db_io_manager import TablePartitionDimension, TableSlice
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import NestedField, StringType

from dagster_iceberg.io_manager.daft import _DaftIcebergTypeHandler
from dagster_iceberg.io_manager.polars import _PolarsIcebergTypeHandler

PARTITION_COLUMN = 'region" = region OR "region'
COLUMNS = [
    "plain",
    "MixedCase",
    "my col",
    'it"s',
    "it's",
    "plain FROM self --",
    "plain FROM ddf --",
]
REGIONS = ["east", "west"]
VALUES = {column: f"{column}/east" for column in COLUMNS} | {PARTITION_COLUMN: "east"}


@pytest.fixture
def names_table(tmp_path: Path) -> Table:
    catalog = SqlCatalog(
        "test",
        uri=f"sqlite:///{tmp_path / 'catalog.db'}",
        warehouse=f"file://{tmp_path / 'warehouse'}",
    )
    catalog.create_namespace("ns")
    names = [PARTITION_COLUMN, *COLUMNS]
    table = catalog.create_table(
        "ns.names",
        schema=Schema(
            *(
                NestedField(field_id, name, StringType(), required=False)
                for field_id, name in enumerate(names, start=1)
            )
        ),
        partition_spec=PartitionSpec(
            PartitionField(
                source_id=1,
                field_id=1000,
                transform=IdentityTransform(),
                name=PARTITION_COLUMN,
            )
        ),
    )
    table.append(
        pa.table(
            {name: [f"{name}/{region}" for region in REGIONS] for name in COLUMNS}
            | {PARTITION_COLUMN: REGIONS}
        )
    )
    return table


def names_slice(column: str) -> TableSlice:
    return TableSlice(
        table="names",
        schema="ns",
        columns=[column],
        partition_dimensions=[
            TablePartitionDimension(
                partition_expr=PARTITION_COLUMN, partitions=["east"]
            )
        ],
    )


@pytest.mark.parametrize("column", [*COLUMNS, PARTITION_COLUMN])
def test_polars_selects_the_named_column_of_the_partition(
    names_table: Table, column: str
):
    frame = _PolarsIcebergTypeHandler().to_data_frame(
        names_table, names_slice(column), pl.DataFrame
    )
    assert frame.to_dict(as_series=False) == {column: [VALUES[column]]}


# Daft looks up Parquet columns by name, and PyIceberg writes names such as "my col"
# under a sanitized name, so Daft can only read such a column from partition values.
@pytest.mark.parametrize("column", ["plain", PARTITION_COLUMN])
def test_daft_selects_the_named_column_of_the_partition(
    names_table: Table, column: str
):
    frame = _DaftIcebergTypeHandler().to_data_frame(
        names_table, names_slice(column), daft.DataFrame
    )
    assert frame.to_pydict() == {column: [VALUES[column]]}

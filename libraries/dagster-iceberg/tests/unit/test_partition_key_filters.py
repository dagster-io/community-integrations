"""Polars and Daft filter static partition keys by their exact values."""

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

KEYS = [
    "plain",
    "it's",
    "back\\slash",
    "x' OR '1'='1",
    "x\\' OR '1'='1",
    "line\nbreak' OR '1'='1",
    "nul\x00byte",
    "\u2019 OR \u20181\u2019=\u20181",
    "*",
]


@pytest.fixture
def keys_table(tmp_path: Path) -> Table:
    catalog = SqlCatalog(
        "test",
        uri=f"sqlite:///{tmp_path / 'catalog.db'}",
        warehouse=f"file://{tmp_path / 'warehouse'}",
    )
    catalog.create_namespace("ns")
    table = catalog.create_table(
        "ns.keys",
        schema=Schema(NestedField(1, "key", StringType(), required=False)),
        partition_spec=PartitionSpec(
            PartitionField(
                source_id=1, field_id=1000, transform=IdentityTransform(), name="key"
            )
        ),
    )
    table.append(pa.table({"key": pa.array(KEYS, pa.string())}))
    return table


def keys_slice(*keys: str) -> TableSlice:
    return TableSlice(
        table="keys",
        schema="ns",
        partition_dimensions=[
            TablePartitionDimension(partition_expr="key", partitions=list(keys))
        ],
    )


@pytest.mark.parametrize("key", KEYS)
def test_polars_static_partition_selects_only_that_key(keys_table: Table, key: str):
    frame = _PolarsIcebergTypeHandler().to_data_frame(
        keys_table, keys_slice(key), pl.DataFrame
    )
    assert frame["key"].to_list() == [key]


@pytest.mark.parametrize("key", KEYS)
def test_daft_static_partition_selects_only_that_key(keys_table: Table, key: str):
    frame = _DaftIcebergTypeHandler().to_data_frame(
        keys_table, keys_slice(key), daft.DataFrame
    )
    assert frame.to_pydict()["key"] == [key]


def test_polars_static_partition_selects_several_keys(keys_table: Table):
    frame = _PolarsIcebergTypeHandler().to_data_frame(
        keys_table, keys_slice("plain", "it's"), pl.DataFrame
    )
    assert sorted(frame["key"].to_list()) == ["it's", "plain"]


def test_daft_static_partition_selects_several_keys(keys_table: Table):
    frame = _DaftIcebergTypeHandler().to_data_frame(
        keys_table, keys_slice("plain", "it's"), daft.DataFrame
    )
    assert sorted(frame.to_pydict()["key"]) == ["it's", "plain"]

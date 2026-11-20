"""The "Query" output metadata quotes names that are not plain identifiers."""

import pytest
from dagster._core.storage.db_io_manager import TablePartitionDimension, TableSlice

from dagster_iceberg.io_manager.base import IcebergDbClient
from dagster_iceberg.io_manager.spark import SparkIcebergDbClient


def table_slice(
    table: str = "t",
    schema: str = "ns",
    columns: list[str] | None = None,
    partition_expr: str | None = None,
    keys: list[str] | None = None,
) -> TableSlice:
    return TableSlice(
        table=table,
        schema=schema,
        columns=columns,
        partition_dimensions=(
            [TablePartitionDimension(partition_expr=partition_expr, partitions=keys)]
            if partition_expr and keys
            else None
        ),
    )


@pytest.mark.parametrize(
    ("table_slice", "query"),
    [
        (table_slice(), "SELECT * FROM ns.t"),
        (table_slice(columns=["a", "b_1"]), "SELECT a, b_1 FROM ns.t"),
        (table_slice(schema="db.ns", columns=["s.f"]), "SELECT s.f FROM db.ns.t"),
        (table_slice(columns=['a" FROM x --']), 'SELECT "a"" FROM x --" FROM ns.t'),
        (table_slice(table="my table"), 'SELECT * FROM ns."my table"'),
        (table_slice(schema="x; DROP"), 'SELECT * FROM "x; DROP".t'),
        (
            table_slice(partition_expr='k"ey', keys=["it's"]),
            "SELECT * FROM ns.t WHERE\n\"k\"\"ey\" in ('it''s')",
        ),
    ],
)
def test_iceberg_select_statement(table_slice: TableSlice, query: str):
    assert IcebergDbClient.get_select_statement(table_slice) == query


@pytest.mark.parametrize(
    ("table_slice", "query"),
    [
        (table_slice(), "SELECT * FROM ns.t"),
        (table_slice(columns=["a", "b_1"]), "SELECT a, b_1 FROM ns.t"),
        (table_slice(schema="db.ns", columns=["s.f"]), "SELECT s.f FROM db.ns.t"),
        (table_slice(columns=["a` FROM x --"]), "SELECT `a`` FROM x --` FROM ns.t"),
        (table_slice(table="my table"), "SELECT * FROM ns.`my table`"),
        (table_slice(schema="x; DROP"), "SELECT * FROM `x; DROP`.t"),
        (
            table_slice(partition_expr="k`ey", keys=["it's"]),
            "SELECT * FROM ns.t WHERE\n`k``ey` in ('it\\'s')",
        ),
    ],
)
def test_spark_select_statement(table_slice: TableSlice, query: str):
    assert SparkIcebergDbClient.get_select_statement(table_slice) == query

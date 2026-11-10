from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import adbc_driver_sqlite
import pandas as pd
import polars as pl
import pyarrow as pa
import pytest
from adbc_driver_manager import dbapi
from dagster import (
    AssetExecutionContext,
    AssetIn,
    DailyPartitionsDefinition,
    StaticPartitionsDefinition,
    asset,
    build_init_resource_context,
    build_output_context,
    materialize,
)
from dagster._core.storage.db_io_manager import TablePartitionDimension, TableSlice

from dagster_adbc import ADBCIOManager, ADBCResource
from dagster_adbc.io_manager import ADBCClient


@pytest.fixture
def manager(tmp_path: Path) -> ADBCIOManager:
    return ADBCIOManager(driver=adbc_driver_sqlite._driver_path(), uri=str(tmp_path / "assets.db"))


def read(manager: ADBCIOManager, sql: str) -> pa.Table:
    resource = ADBCResource(
        **{field: getattr(manager, field) for field in ADBCResource.model_fields}
    )
    with resource.get_connection() as connection, connection.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetch_arrow_table()


def staging_tables(manager: ADBCIOManager) -> list[str]:
    resource = ADBCResource(
        **{field: getattr(manager, field) for field in ADBCResource.model_fields}
    )
    names = []
    with resource.get_connection() as connection:
        with connection.adbc_get_objects(
            depth="tables",
            db_schema_filter=manager.schema_,
        ) as reader:
            for catalog in reader.read_all().to_pylist():
                for schema in catalog["catalog_db_schemas"] or []:
                    for table in schema["db_schema_tables"] or []:
                        if table["table_name"].startswith("__dagster_staging_"):
                            names.append(table["table_name"])
    return names


@pytest.mark.parametrize("frame_type", [pa.Table, pd.DataFrame, pl.DataFrame])
def test_round_trip_and_columns(manager: ADBCIOManager, frame_type: type) -> None:
    values = {"value": [1, 2], "other": ["a", "b"]}

    @asset
    def upstream() -> frame_type:
        return pa.table(values) if frame_type is pa.Table else frame_type(values)

    @asset(ins={"upstream": AssetIn(metadata={"columns": ["value"]})})
    def downstream(upstream: frame_type) -> pa.Table:
        if isinstance(upstream, pa.Table):
            assert upstream.to_pydict() == {"value": [1, 2]}
        else:
            assert list(upstream.columns) == ["value"]
            assert upstream["value"].to_list() == [1, 2]
        return pa.table({"seen": [2]})

    for _ in range(2):
        assert materialize([upstream, downstream], resources={"io_manager": manager}).success
    assert read(manager, 'SELECT * FROM "upstream"').to_pydict() == values
    assert staging_tables(manager) == []


@pytest.mark.parametrize("time_partition", [False, True])
def test_partition_replacement(manager: ADBCIOManager, time_partition: bool) -> None:
    partitions = (
        DailyPartitionsDefinition(start_date="2026-01-01")
        if time_partition
        else StaticPartitionsDefinition(["a'b", "other"])
    )
    first, second = ("2026-01-01", "2026-01-02") if time_partition else ("a'b", "other")
    version = 0

    @asset(partitions_def=partitions, metadata={"partition_expr": '"partition"'})
    def partitioned(context: AssetExecutionContext) -> pa.Table:
        key: datetime | str = (
            datetime.fromisoformat(context.partition_key)
            if time_partition
            else context.partition_key
        )
        return pa.table({"partition": [key], "value": [version]})

    @asset(partitions_def=partitions, metadata={"partition_expr": '"partition"'})
    def downstream(partitioned: pa.Table) -> pa.Table:
        assert partitioned.num_rows == 1
        assert partitioned["value"][0].as_py() == version
        return partitioned

    for key in [first, second, first]:
        version += 1
        assert materialize(
            [partitioned, downstream], resources={"io_manager": manager}, partition_key=key
        ).success
    rows = read(manager, 'SELECT * FROM "partitioned" ORDER BY "value"').to_pydict()
    assert rows["value"] == [2, 3]


def test_failed_partition_ingestion_rolls_back(manager: ADBCIOManager) -> None:
    bad = False

    @asset(
        partitions_def=StaticPartitionsDefinition(["a", "b"]),
        metadata={"partition_expr": '"partition"'},
    )
    def partitioned(context: AssetExecutionContext) -> pa.Table:
        return pa.table({"partition": [context.partition_key], "wrong" if bad else "value": [1]})

    for key in ["a", "b"]:
        assert materialize(
            [partitioned], resources={"io_manager": manager}, partition_key=key
        ).success
    bad = True
    result = materialize(
        [partitioned], resources={"io_manager": manager}, partition_key="a", raise_on_error=False
    )
    assert not result.success
    assert read(
        manager, 'SELECT "partition" FROM "partitioned" ORDER BY "partition"'
    ).to_pydict() == {"partition": ["a", "b"]}


@pytest.mark.parametrize("autocommit", [False, True])
def test_failed_staging_preserves_output(
    manager: ADBCIOManager, monkeypatch: pytest.MonkeyPatch, autocommit: bool
) -> None:
    manager = manager.model_copy(update={"autocommit": autocommit})
    io_manager = manager.create_io_manager(build_init_resource_context())
    with build_output_context(name='table"name') as context:
        io_manager.handle_output(context, pa.table({"value": [1]}))
    original = dbapi.Cursor.adbc_ingest

    def fail_after_ingestion(self: dbapi.Cursor, *args: object, **kwargs: object) -> int:
        original(self, *args, **kwargs)
        raise RuntimeError("failed staging")

    monkeypatch.setattr(dbapi.Cursor, "adbc_ingest", fail_after_ingestion)
    with build_output_context(name='table"name') as context:
        with pytest.raises(RuntimeError, match="failed staging"):
            io_manager.handle_output(context, pa.table({"value": [2]}))
    assert read(manager, 'SELECT * FROM "table""name"').to_pydict() == {"value": [1]}
    assert staging_tables(manager) == []


@pytest.mark.parametrize("autocommit", [False, True])
def test_swap_failure_and_autocommit_cleanup(
    manager: ADBCIOManager, monkeypatch: pytest.MonkeyPatch, autocommit: bool
) -> None:
    manager = manager.model_copy(update={"autocommit": autocommit})
    io_manager = manager.create_io_manager(build_init_resource_context())
    with build_output_context(name="data") as context:
        io_manager.handle_output(context, pa.table({"value": [1]}))
    original = dbapi.Cursor.execute

    def fail_rename(self: dbapi.Cursor, operation: str, *args: object, **kwargs: object) -> object:
        if operation.startswith("ALTER TABLE"):
            raise RuntimeError("rename failed")
        return original(self, operation, *args, **kwargs)

    monkeypatch.setattr(dbapi.Cursor, "execute", fail_rename)
    with build_output_context(name="data") as context:
        with pytest.raises(RuntimeError, match="rename failed"):
            io_manager.handle_output(context, pa.table({"value": [2]}))
    assert staging_tables(manager) == []
    if not autocommit:
        assert read(manager, 'SELECT * FROM "data"').to_pydict() == {"value": [1]}


def test_sql_dialect_and_empty_partition() -> None:
    client = ADBCClient(ADBCIOManager(dialect="mysql", use_schema=True))
    table_slice = TableSlice(
        table="a`b",
        schema="s",
        database="d",
        columns=["select"],
        partition_dimensions=[TablePartitionDimension("region", ["a'b"])],
    )
    assert client.get_select_statement(table_slice) == (
        "SELECT `select` FROM `d`.`s`.`a``b` WHERE (region IN ('a''b'))"
    )
    empty = table_slice._replace(partition_dimensions=[TablePartitionDimension("region", [])])
    assert client.get_select_statement(empty).endswith("WHERE (1 = 0)")
    raw = MagicMock()
    client.ensure_schema_exists(MagicMock(), table_slice, MagicMock(raw=raw))
    raw.cursor.return_value.__enter__.return_value.execute.assert_called_with(
        "CREATE SCHEMA IF NOT EXISTS `d`.`s`"
    )


@pytest.mark.parametrize("effective_autocommit", [False, True])
@pytest.mark.parametrize("failure", [False, True])
def test_effective_transaction_fallback(
    monkeypatch: pytest.MonkeyPatch, effective_autocommit: bool, failure: bool
) -> None:
    from adbc_driver_manager import NotSupportedError

    raw = MagicMock()
    raw.adbc_get_info.return_value = {"vendor_name": "SQLite"}
    raw.adbc_connection.get_option.side_effect = NotSupportedError("option not implemented")
    raw._autocommit = effective_autocommit
    monkeypatch.setattr(dbapi, "connect", lambda **kwargs: raw)
    client = ADBCClient(ADBCIOManager(driver="test"))
    table_slice = TableSlice(table="data", schema="public")
    with build_output_context() as context:
        if failure:
            with pytest.raises(RuntimeError, match="write failed"):
                with client.connect(context, table_slice):
                    raise RuntimeError("write failed")
        else:
            with client.connect(context, table_slice):
                pass
    assert raw.commit.call_count == int(not failure and not effective_autocommit)
    assert raw.rollback.call_count == int(failure and not effective_autocommit)
    raw.close.assert_called_once()


def test_multi_partitions(manager: ADBCIOManager) -> None:
    from dagster import MultiPartitionKey, MultiPartitionsDefinition

    partitions = MultiPartitionsDefinition(
        {
            "region": StaticPartitionsDefinition(["east", "west"]),
            "day": DailyPartitionsDefinition(start_date="2026-01-01"),
        }
    )
    version = 0

    @asset(
        partitions_def=partitions,
        metadata={"partition_expr": {"region": '"region"', "day": '"day"'}},
    )
    def partitioned(context: AssetExecutionContext) -> pa.Table:
        key = context.partition_key.keys_by_dimension
        return pa.table(
            {
                "region": [key["region"]],
                "day": [datetime.fromisoformat(key["day"])],
                "value": [version],
            }
        )

    for region, day in [
        ("east", "2026-01-01"),
        ("west", "2026-01-01"),
        ("east", "2026-01-02"),
        ("east", "2026-01-01"),
    ]:
        version += 1
        assert materialize(
            [partitioned],
            resources={"io_manager": manager},
            partition_key=MultiPartitionKey({"region": region, "day": day}),
        ).success
    assert read(manager, "SELECT value FROM partitioned ORDER BY value").to_pydict() == {
        "value": [2, 3, 4]
    }

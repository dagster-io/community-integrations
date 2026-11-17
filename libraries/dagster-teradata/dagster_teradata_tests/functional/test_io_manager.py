"""Functional tests for ``TeradataIOManager`` against a live Teradata database.

These tests are skipped unless the standard Teradata environment variables are
set::

    TERADATA_HOST, TERADATA_USER, TERADATA_PASSWORD, TERADATA_DATABASE

Run them with::

    pytest dagster_teradata_tests/functional/test_io_manager.py -v

The pandas type handler used here is :py:class:`~dagster_teradata.TeradataPandasTypeHandler`,
shipped as part of ``dagster_teradata`` (IDE-26551). These tests exercise it
end to end against a live Teradata instance.
"""

import os
from datetime import date, datetime
from decimal import Decimal

import pandas as pd
import pytest
import teradatasql
from dagster import (
    AssetIn,
    DailyPartitionsDefinition,
    StaticPartitionsDefinition,
    asset,
    materialize,
)

from dagster_teradata import (
    TeradataPandasIOManager,
    TeradataPandasTypeHandler,
    TeradataResource,
)

REQUIRED_ENV_VARS = [
    "TERADATA_HOST",
    "TERADATA_USER",
    "TERADATA_PASSWORD",
    "TERADATA_DATABASE",
]

pytestmark = pytest.mark.skipif(
    any(not os.getenv(var) for var in REQUIRED_ENV_VARS),
    reason=f"Set {', '.join(REQUIRED_ENV_VARS)} to run live Teradata tests.",
)


@pytest.fixture(scope="module")
def teradata_resource() -> TeradataResource:
    return TeradataResource(
        host=os.getenv("TERADATA_HOST"),
        user=os.getenv("TERADATA_USER"),
        password=os.getenv("TERADATA_PASSWORD"),
        database=os.getenv("TERADATA_DATABASE"),
    )


@pytest.fixture(scope="module")
def connection(teradata_resource: TeradataResource):
    # TeradataResource.get_connection() does not close the underlying connection on
    # exit, so close it explicitly here to avoid leaking a Teradata session for the
    # duration of the test process.
    with teradata_resource.get_connection() as con:
        try:
            yield con
        finally:
            con.close()


@pytest.fixture
def io_manager(teradata_resource: TeradataResource) -> TeradataPandasIOManager:
    return TeradataPandasIOManager(teradata=teradata_resource)


def _drop_table(connection, table: str) -> None:
    database = os.getenv("TERADATA_DATABASE")
    try:
        connection.cursor().execute(f'DROP TABLE "{database}"."{table}"')
    except teradatasql.DatabaseError:
        pass


def _read_all(connection, table: str) -> pd.DataFrame:
    database = os.getenv("TERADATA_DATABASE")
    cursor = connection.cursor()
    cursor.execute(f'SELECT * FROM "{database}"."{table}"')
    return pd.DataFrame(cursor.fetchall(), columns=[d[0] for d in cursor.description])


def test_connection_smoke(connection):
    cursor = connection.cursor()
    cursor.execute("SELECT DATABASE, USER")
    assert cursor.fetchall()


def test_unpartitioned_round_trip(connection, io_manager):
    _drop_table(connection, "io_basic")
    _drop_table(connection, "io_basic_downstream")

    @asset(name="io_basic")
    def io_basic() -> pd.DataFrame:
        return pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})

    @asset(name="io_basic_downstream")
    def io_basic_downstream(io_basic: pd.DataFrame) -> pd.DataFrame:
        assert sorted(io_basic["a"].tolist()) == [1, 2, 3]
        return io_basic

    try:
        assert materialize(
            [io_basic, io_basic_downstream], resources={"io_manager": io_manager}
        ).success
        assert len(_read_all(connection, "io_basic")) == 3
    finally:
        _drop_table(connection, "io_basic")
        _drop_table(connection, "io_basic_downstream")


def test_rematerialize_replaces_rows(connection, io_manager):
    """The unpartitioned DELETE cleanup path must not duplicate rows."""
    _drop_table(connection, "io_replace")

    @asset(name="io_replace")
    def io_replace() -> pd.DataFrame:
        return pd.DataFrame({"a": [1, 2]})

    try:
        assert materialize([io_replace], resources={"io_manager": io_manager}).success
        assert materialize([io_replace], resources={"io_manager": io_manager}).success
        assert len(_read_all(connection, "io_replace")) == 2
    finally:
        _drop_table(connection, "io_replace")


def test_column_subsetting(connection, io_manager):
    _drop_table(connection, "io_cols")
    _drop_table(connection, "io_cols_downstream")

    @asset(name="io_cols")
    def io_cols() -> pd.DataFrame:
        return pd.DataFrame({"a": [1, 2], "b": ["p", "q"]})

    @asset(
        name="io_cols_downstream",
        ins={"io_cols": AssetIn("io_cols", metadata={"columns": ["a"]})},
    )
    def io_cols_downstream(io_cols: pd.DataFrame) -> pd.DataFrame:
        assert list(io_cols.columns) == ["a"]
        return io_cols

    try:
        assert materialize(
            [io_cols, io_cols_downstream], resources={"io_manager": io_manager}
        ).success
    finally:
        _drop_table(connection, "io_cols")
        _drop_table(connection, "io_cols_downstream")


def test_static_partitions(connection, io_manager):
    """Exercises the escaped IN (...) partition cleanup path."""
    _drop_table(connection, "io_static")

    partitions = StaticPartitionsDefinition(["red", "blue"])

    @asset(
        name="io_static",
        partitions_def=partitions,
        metadata={"partition_expr": "color"},
    )
    def io_static(context) -> pd.DataFrame:
        color = context.partition_key
        return pd.DataFrame({"color": [color, color], "n": [1, 2]})

    try:
        for key in ["red", "blue"]:
            assert materialize(
                [io_static], resources={"io_manager": io_manager}, partition_key=key
            ).success
        assert len(_read_all(connection, "io_static")) == 4

        # Re-materializing one partition must replace only that partition.
        assert materialize(
            [io_static], resources={"io_manager": io_manager}, partition_key="red"
        ).success
        table = _read_all(connection, "io_static")
        assert len(table) == 4
        assert sorted(table["color"].tolist()) == ["blue", "blue", "red", "red"]
    finally:
        _drop_table(connection, "io_static")


def test_time_window_partitions(connection, io_manager):
    """Exercises the CAST(... AS TIMESTAMP(6)) time-window WHERE clause."""
    _drop_table(connection, "io_daily")

    partitions = DailyPartitionsDefinition(start_date="2024-01-01")

    @asset(
        name="io_daily",
        partitions_def=partitions,
        metadata={"partition_expr": "ts"},
    )
    def io_daily(context) -> pd.DataFrame:
        day = datetime.fromisoformat(context.partition_key)
        return pd.DataFrame({"ts": [day, day], "n": [1, 2]})

    try:
        for key in ["2024-01-01", "2024-01-02"]:
            assert materialize(
                [io_daily], resources={"io_manager": io_manager}, partition_key=key
            ).success
        assert len(_read_all(connection, "io_daily")) == 4

        assert materialize(
            [io_daily], resources={"io_manager": io_manager}, partition_key="2024-01-01"
        ).success
        assert len(_read_all(connection, "io_daily")) == 4
    finally:
        _drop_table(connection, "io_daily")


def test_schema_falls_back_to_resource_database(connection, io_manager):
    """No key prefix and no schema config -> resource database, not 'public'."""
    _drop_table(connection, "io_fallback")

    @asset(name="io_fallback")
    def io_fallback() -> pd.DataFrame:
        return pd.DataFrame({"a": [1]})

    try:
        assert materialize([io_fallback], resources={"io_manager": io_manager}).success
        assert len(_read_all(connection, "io_fallback")) == 1
    finally:
        _drop_table(connection, "io_fallback")


def test_missing_database_raises(teradata_resource):
    """A nonexistent database must produce an actionable error, not 'public'."""

    @asset(key_prefix=["definitely_not_a_real_database_xyz"], name="io_missing")
    def io_missing() -> pd.DataFrame:
        return pd.DataFrame({"a": [1]})

    result = materialize(
        [io_missing],
        resources={"io_manager": TeradataPandasIOManager(teradata=teradata_resource)},
        raise_on_error=False,
    )
    assert not result.success


def test_dtype_mapping_round_trip(connection, io_manager):
    """The shipped dtype mapping must produce columns Teradata actually accepts."""
    _drop_table(connection, "io_dtypes")

    frame = pd.DataFrame(
        {
            "small_int": pd.Series([1, 2], dtype="int16"),
            "big_int": pd.Series([2**40, 1], dtype="int64"),
            "real": pd.Series([1.5, None], dtype="float64"),
            "flag": pd.Series([True, False]),
            "text": pd.Series(["hello", None]),
            "amount": pd.Series([Decimal("12.345"), Decimal("0.001")], dtype=object),
            "day": pd.Series([date(2024, 1, 1), date(2024, 1, 2)], dtype=object),
            "moment": pd.to_datetime(["2024-01-01 10:00:00", None]),
        }
    )

    @asset(name="io_dtypes")
    def io_dtypes() -> pd.DataFrame:
        return frame

    try:
        assert materialize([io_dtypes], resources={"io_manager": io_manager}).success

        database = os.getenv("TERADATA_DATABASE")
        cursor = connection.cursor()
        cursor.execute(
            "SELECT ColumnName, ColumnType FROM DBC.ColumnsV "
            "WHERE UPPER(DatabaseName) = UPPER(?) AND UPPER(TableName) = UPPER(?)",
            [database, "io_dtypes"],
        )
        column_types = {
            name.strip().lower(): type_code.strip()
            for name, type_code in cursor.fetchall()
        }
        # Teradata type codes: I2=SMALLINT, I8=BIGINT, F=FLOAT, D=DECIMAL, DA=DATE,
        # TS=TIMESTAMP, I1=BYTEINT, CV=VARCHAR.
        assert column_types["small_int"] == "I2"
        assert column_types["big_int"] == "I8"
        assert column_types["real"] == "F"
        assert column_types["flag"] == "I1"
        assert column_types["amount"] == "D"
        assert column_types["day"] == "DA"
        assert column_types["moment"] == "TS"

        stored = _read_all(connection, "io_dtypes")
        assert len(stored) == 2
    finally:
        _drop_table(connection, "io_dtypes")


def test_chunked_writes(connection, teradata_resource):
    """Chunking must insert every row exactly once."""
    _drop_table(connection, "io_chunks")

    io_manager = TeradataPandasIOManager(teradata=teradata_resource, chunk_size=250)

    @asset(name="io_chunks")
    def io_chunks() -> pd.DataFrame:
        return pd.DataFrame({"n": range(1000)})

    try:
        assert materialize([io_chunks], resources={"io_manager": io_manager}).success
        stored = _read_all(connection, "io_chunks")
        assert len(stored) == 1000
        assert sorted(int(n) for n in stored["n"]) == list(range(1000))
    finally:
        _drop_table(connection, "io_chunks")


def test_handler_is_the_shipped_one(io_manager):
    (handler,) = io_manager.type_handlers()
    assert isinstance(handler, TeradataPandasTypeHandler)

"""Unit tests for the pandas type handler, using a mocked Teradata connection."""

import datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pandas as pd
import pytest
import teradatasql
from dagster._core.storage.db_io_manager import TableSlice

import dagster_teradata
from dagster_teradata import TeradataResource
from dagster_teradata.pandas_type_handler import (
    DEFAULT_CHUNK_SIZE,
    TeradataPandasIOManager,
    TeradataPandasTypeHandler,
)


@pytest.fixture
def handler() -> TeradataPandasTypeHandler:
    return TeradataPandasTypeHandler()


def make_table_slice(**kwargs) -> TableSlice:
    defaults = {"table": "my_table", "schema": "my_db", "database": None}
    defaults.update(kwargs)
    return TableSlice(**defaults)


def _cursor(connection: MagicMock) -> MagicMock:
    return connection.cursor.return_value.__enter__.return_value


def _executed(cursor: MagicMock) -> list:
    return [call.args[0] for call in cursor.execute.call_args_list]


# --------------------------------------------------------------------------------------
# dtype mapping
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("series", "expected"),
    [
        (pd.Series([1, 2], dtype="int8"), "BYTEINT"),
        (pd.Series([1, 2], dtype="int16"), "SMALLINT"),
        (pd.Series([1, 2], dtype="int32"), "INTEGER"),
        (pd.Series([1, 2], dtype="int64"), "BIGINT"),
        (pd.Series([1, None], dtype="Int32"), "INTEGER"),
        (pd.Series([1.5], dtype="float64"), "FLOAT"),
        (pd.Series([True, False]), "BYTEINT"),
        (pd.Series(pd.to_datetime(["2024-01-01"])), "TIMESTAMP(6)"),
        (
            pd.Series(pd.to_datetime(["2024-01-01"]).tz_localize("UTC")),
            "TIMESTAMP(6) WITH TIME ZONE",
        ),
    ],
)
def test_column_type_mapping(handler, series, expected):
    assert handler.column_type("col", series) == expected


def test_varchar_width_has_headroom_and_floor(handler):
    short = pd.Series(["ab"])
    assert handler.column_type("col", short) == "VARCHAR(256) CHARACTER SET UNICODE"

    long_value = pd.Series(["x" * 400])
    assert (
        handler.column_type("col", long_value) == "VARCHAR(800) CHARACTER SET UNICODE"
    )


def test_very_wide_strings_become_clob(handler):
    wide = pd.Series(["x" * 20_000])
    assert handler.column_type("col", wide) == "CLOB CHARACTER SET UNICODE"


def test_categorical_column_sized_from_full_category_domain():
    # The current frame only contains the short category, but a later batch may
    # contain "long_category_value" since it is already part of the declared domain.
    handler = TeradataPandasTypeHandler(min_varchar_length=10)
    series = pd.Series(
        ["short"],
        dtype=pd.CategoricalDtype(categories=["short", "long_category_value"]),
    )
    assert handler.column_type("col", series) == "VARCHAR(38) CHARACTER SET UNICODE"


def test_decimal_columns_keep_precision_and_scale(handler):
    series = pd.Series([Decimal("12.345"), Decimal("6.7")], dtype=object)
    assert handler.column_type("amount", series) == "DECIMAL(5,3)"


def test_decimal_type_fits_values_with_different_scales(handler):
    # 999.9 needs 3 integer digits and 0.001 needs 3 fractional digits; a type
    # must accommodate both, not just the max precision and max scale in isolation.
    series = pd.Series([Decimal("999.9"), Decimal("0.001")], dtype=object)
    assert handler.column_type("amount", series) == "DECIMAL(6,3)"


def test_decimal_type_accounts_for_positive_exponents(handler):
    series = pd.Series([Decimal("1.5E+3")], dtype=object)
    assert handler.column_type("amount", series) == "DECIMAL(4,0)"


def test_decimal_type_rejects_values_beyond_max_precision(handler):
    # 1E+38 needs 39 integer digits, one more than Teradata's max DECIMAL precision;
    # silently clamping the precision would produce a type too narrow for the value.
    series = pd.Series([Decimal("1E+38")], dtype=object)
    with pytest.raises(
        ValueError, match="exceeds Teradata's maximum DECIMAL precision"
    ):
        handler.column_type("amount", series)


@pytest.mark.parametrize(
    ("dtype", "expected"),
    [
        ("uint8", "SMALLINT"),
        ("uint16", "INTEGER"),
        ("uint32", "BIGINT"),
        ("uint64", "DECIMAL(20,0)"),
    ],
)
def test_unsigned_integer_dtypes_widen_to_fit(handler, dtype, expected):
    assert handler.column_type("col", pd.Series([1], dtype=dtype)) == expected


def test_date_and_time_objects(handler):
    dates = pd.Series([datetime.date(2024, 1, 1)], dtype=object)
    times = pd.Series([datetime.time(12, 30)], dtype=object)
    assert handler.column_type("d", dates) == "DATE"
    assert handler.column_type("t", times) == "TIME(6)"


def test_bytes_become_blob(handler):
    series = pd.Series([b"\x00\x01"], dtype=object)
    assert handler.column_type("payload", series) == "BLOB"


def test_all_null_object_column_falls_back_to_varchar(handler):
    series = pd.Series([None, None], dtype=object)
    assert handler.column_type("col", series) == "VARCHAR(256) CHARACTER SET UNICODE"


def test_all_null_object_column_becomes_clob_when_min_length_exceeds_limit():
    handler = TeradataPandasTypeHandler(min_varchar_length=32_001)
    series = pd.Series([None, None], dtype=object)
    assert handler.column_type("col", series) == "CLOB CHARACTER SET UNICODE"


def test_timedelta_raises_actionable_error(handler):
    series = pd.Series(pd.to_timedelta(["1 days"]))
    with pytest.raises(ValueError, match="unsupported timedelta dtype"):
        handler.column_type("elapsed", series)


def test_explicit_column_types_override_inference():
    handler = TeradataPandasTypeHandler(column_types={"id": "DECIMAL(38,0)"})
    assert handler.column_type("id", pd.Series([1], dtype="int64")) == "DECIMAL(38,0)"


def test_min_varchar_length_is_configurable():
    handler = TeradataPandasTypeHandler(min_varchar_length=10)
    assert (
        handler.column_type("col", pd.Series(["ab"]))
        == "VARCHAR(10) CHARACTER SET UNICODE"
    )


def test_invalid_constructor_arguments():
    with pytest.raises(ValueError, match="chunk_size"):
        TeradataPandasTypeHandler(chunk_size=0)
    with pytest.raises(ValueError, match="min_varchar_length"):
        TeradataPandasTypeHandler(min_varchar_length=0)


# --------------------------------------------------------------------------------------
# handle_output
# --------------------------------------------------------------------------------------


def test_handle_output_creates_table_and_inserts(handler):
    connection = MagicMock()
    cursor = _cursor(connection)
    cursor.fetchone.return_value = None  # table_exists check: no matching row
    obj = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})

    metadata = handler.handle_output(MagicMock(), make_table_slice(), obj, connection)

    create = next(stmt for stmt in _executed(cursor) if stmt.startswith("CREATE TABLE"))
    assert create == (
        'CREATE TABLE "my_db"."my_table" ("a" BIGINT, "b" VARCHAR(256) CHARACTER SET UNICODE) '
        "NO PRIMARY INDEX"
    )
    statement, rows = cursor.executemany.call_args.args
    assert statement == 'INSERT INTO "my_db"."my_table" ("a", "b") VALUES (?, ?)'
    assert rows == [[1, "x"], [2, "y"]]
    assert metadata["row_count"] == 2
    # A DDL statement must be immediately committed (Teradata requires COMMIT WORK
    # or a null statement to follow DDL), so the INSERT that follows is legal.
    connection.commit.assert_called_once()


def test_handle_output_skips_create_table_when_already_exists(handler):
    """When the table already exists, CREATE TABLE must not be attempted at all -
    only DML runs, so the DELETE done by delete_table_slice() and this INSERT can
    stay in one uninterrupted transaction (Teradata forbids further statements in a
    transaction after a DDL statement, which is what "[Error 3722] Only a COMMIT
    WORK or null statement is legal after a DDL Statement" enforces)."""
    connection = MagicMock()
    cursor = _cursor(connection)
    cursor.fetchone.return_value = (1,)  # table_exists check finds a matching row
    obj = pd.DataFrame({"a": [1, 2]})

    handler.handle_output(MagicMock(), make_table_slice(), obj, connection)

    assert not any(stmt.startswith("CREATE TABLE") for stmt in _executed(cursor))
    cursor.executemany.assert_called_once()
    connection.commit.assert_not_called()


def test_handle_output_ignores_table_already_exists(handler):
    """A CREATE TABLE that fails with 'already exists' does not force the
    post-DDL commit-only restriction, so no commit should happen here - but a
    failed CREATE TABLE also silently discards delete_table_slice()'s earlier
    DELETE (verified empirically against a live Teradata system), so the DELETE
    must be re-issued before the INSERT, and both stay in one transaction that
    TeradataDbClient.connect() commits or rolls back as a whole."""
    connection = MagicMock()
    cursor = _cursor(connection)

    def execute(statement, *args, **kwargs):
        if statement.startswith("CREATE TABLE"):
            raise teradatasql.DatabaseError(
                "[Error 3803] Table 'my_table' already exists."
            )

    cursor.execute.side_effect = execute
    cursor.fetchone.return_value = None  # table_exists check: no matching row

    handler.handle_output(
        MagicMock(), make_table_slice(), pd.DataFrame({"a": [1]}), connection
    )

    executed = _executed(cursor)
    assert executed.count('DELETE FROM "my_db"."my_table"') == 1
    cursor.executemany.assert_called_once()
    connection.commit.assert_not_called()


def test_handle_output_propagates_other_create_errors(handler):
    connection = MagicMock()
    _cursor(connection).execute.side_effect = teradatasql.DatabaseError(
        "[Error 3523] insufficient privilege"
    )

    with pytest.raises(teradatasql.DatabaseError):
        handler.handle_output(
            MagicMock(), make_table_slice(), pd.DataFrame({"a": [1]}), connection
        )


def test_handle_output_chunks_large_frames():
    handler = TeradataPandasTypeHandler(chunk_size=2)
    connection = MagicMock()
    obj = pd.DataFrame({"a": range(5)})

    handler.handle_output(MagicMock(), make_table_slice(), obj, connection)

    batches = [call.args[1] for call in _cursor(connection).executemany.call_args_list]
    assert [len(batch) for batch in batches] == [2, 2, 1]
    assert [row[0] for batch in batches for row in batch] == [0, 1, 2, 3, 4]


def test_handle_output_converts_nan_and_timestamps(handler):
    connection = MagicMock()
    obj = pd.DataFrame(
        {
            "n": [1.0, None],
            "s": ["a", None],
            "ts": pd.to_datetime(["2024-01-01 10:00:00", None]),
        }
    )

    handler.handle_output(MagicMock(), make_table_slice(), obj, connection)

    rows = _cursor(connection).executemany.call_args.args[1]
    assert rows[0] == [1.0, "a", datetime.datetime(2024, 1, 1, 10, 0)]
    assert rows[1] == [None, None, None]
    # numpy scalars must be converted to plain Python types for the driver.
    assert type(rows[0][0]) is float


def test_handle_output_stringifies_unsupported_objects_in_varchar_columns(handler):
    # A dict/tuple/etc. in an object column falls back to VARCHAR DDL, so the value
    # sent to the driver must be a string, not the original unsupported object.
    connection = MagicMock()
    obj = pd.DataFrame({"payload": [{"a": 1}, (1, 2), None]})

    handler.handle_output(MagicMock(), make_table_slice(), obj, connection)

    rows = _cursor(connection).executemany.call_args.args[1]
    assert rows == [["{'a': 1}"], ["(1, 2)"], [None]]
    assert all(value is None or isinstance(value, str) for row in rows for value in row)


@pytest.mark.parametrize(
    "override",
    [
        "CHAR(20)",
        "CHARACTER(20)",
        "CHAR VARYING(20)",
        "CHARACTER VARYING(20)",
        "varchar(20) CHARACTER SET UNICODE",
        "LONG VARCHAR",
        "CLOB(1K)",
        "CHARACTER LARGE OBJECT",
    ],
)
def test_handle_output_stringifies_objects_for_character_overrides(override):
    # Every spelling of a character type must be stringified, or the driver is
    # handed the raw object (a dict here) and fails to bind it.
    handler = TeradataPandasTypeHandler(column_types={"payload": override})
    connection = MagicMock()
    obj = pd.DataFrame({"payload": [{"a": 1}, None]})

    handler.handle_output(MagicMock(), make_table_slice(), obj, connection)

    rows = _cursor(connection).executemany.call_args.args[1]
    assert rows == [["{'a': 1}"], [None]]


def test_handle_output_leaves_non_string_columns_unconverted(handler):
    # Columns inferred as a non-string type (e.g. FLOAT here) must not be stringified.
    connection = MagicMock()
    obj = pd.DataFrame({"n": [1.5, 2.5]})

    handler.handle_output(MagicMock(), make_table_slice(), obj, connection)

    rows = _cursor(connection).executemany.call_args.args[1]
    assert rows == [[1.5], [2.5]]


def test_handle_output_skips_insert_for_empty_frame(handler):
    connection = MagicMock()
    obj = pd.DataFrame({"a": pd.Series([], dtype="int64")})

    metadata = handler.handle_output(MagicMock(), make_table_slice(), obj, connection)

    _cursor(connection).executemany.assert_not_called()
    assert metadata["row_count"] == 0


def test_handle_output_rejects_frame_without_columns(handler):
    with pytest.raises(ValueError, match="no columns"):
        handler.handle_output(
            MagicMock(), make_table_slice(), pd.DataFrame(), MagicMock()
        )


def test_handle_output_rejects_duplicate_column_names(handler):
    obj = pd.DataFrame([[1, 2]])
    obj.columns = ["a", "a"]
    with pytest.raises(ValueError, match="duplicate column names"):
        handler.handle_output(MagicMock(), make_table_slice(), obj, MagicMock())


def test_handle_output_rejects_case_insensitive_duplicate_column_names(handler):
    # Teradata identifiers are case-insensitive, so "A" and "a" collide even though
    # pandas treats them as distinct column labels.
    obj = pd.DataFrame({"A": [1], "a": [2]})
    with pytest.raises(ValueError, match="only differ by type or letter case"):
        handler.handle_output(MagicMock(), make_table_slice(), obj, MagicMock())


def test_handle_output_rejects_mixed_type_duplicate_column_names(handler):
    obj = pd.DataFrame([[1, 2]])
    obj.columns = [1, "1"]
    with pytest.raises(ValueError, match="only differ by type or letter case"):
        handler.handle_output(MagicMock(), make_table_slice(), obj, MagicMock())


def test_handle_output_rejects_non_dataframe(handler):
    with pytest.raises(TypeError, match="only store pandas DataFrames"):
        handler.handle_output(MagicMock(), make_table_slice(), [1, 2], MagicMock())


# --------------------------------------------------------------------------------------
# load_input
# --------------------------------------------------------------------------------------


def test_load_input_uses_partition_aware_select(handler):
    connection = MagicMock()
    cursor = _cursor(connection)
    cursor.description = [("a", None), ("b", None)]
    cursor.fetchall.return_value = [[1, "x"], [2, "y"]]

    result = handler.load_input(
        MagicMock(), make_table_slice(columns=["a", "b"]), connection
    )

    assert _executed(cursor) == ['SELECT "a", "b" FROM "my_db"."my_table"']
    pd.testing.assert_frame_equal(result, pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}))


def test_load_input_returns_empty_frame_with_columns(handler):
    connection = MagicMock()
    cursor = _cursor(connection)
    cursor.description = [("a", None)]
    cursor.fetchall.return_value = []

    result = handler.load_input(MagicMock(), make_table_slice(), connection)

    assert result.empty
    assert list(result.columns) == ["a"]


def test_supported_types(handler):
    assert handler.supported_types == [pd.DataFrame]


# --------------------------------------------------------------------------------------
# I/O manager wiring
# --------------------------------------------------------------------------------------


def test_io_manager_passes_configuration_to_handler():
    io_manager = TeradataPandasIOManager(
        teradata=TeradataResource(host="localhost", user="dbc", password="dbc"),
        chunk_size=17,
        min_varchar_length=42,
        column_types={"amount": "DECIMAL(18,4)"},
    )
    (handler,) = io_manager.type_handlers()
    assert handler.chunk_size == 17
    assert handler.min_varchar_length == 42
    assert handler.column_types == {"amount": "DECIMAL(18,4)"}
    assert io_manager.default_load_type() is pd.DataFrame


def test_io_manager_column_types_override_inference():
    """The documented escape hatch must reach the CREATE TABLE statement."""
    io_manager = TeradataPandasIOManager(
        teradata=TeradataResource(host="localhost", user="dbc", password="dbc"),
        column_types={"amount": "DECIMAL(18,4)"},
    )
    (handler,) = io_manager.type_handlers()
    frame = pd.DataFrame({"amount": [1.5], "other": [1.5]})
    assert handler.column_type("amount", frame["amount"]) == "DECIMAL(18,4)"
    assert handler.column_type("other", frame["other"]) == "FLOAT"


def test_io_manager_defaults():
    io_manager = TeradataPandasIOManager(
        teradata=TeradataResource(host="localhost", user="dbc", password="dbc"),
    )
    (handler,) = io_manager.type_handlers()
    assert handler.chunk_size == DEFAULT_CHUNK_SIZE


def test_lazy_exports_available_from_package_root():
    assert dagster_teradata.TeradataPandasIOManager is TeradataPandasIOManager
    assert dagster_teradata.TeradataPandasTypeHandler is TeradataPandasTypeHandler
    assert "TeradataPandasIOManager" in dir(dagster_teradata)


def test_unknown_attribute_still_raises_attribute_error():
    with pytest.raises(AttributeError, match="no attribute 'does_not_exist'"):
        getattr(dagster_teradata, "does_not_exist")  # noqa: B009


def test_uint64_above_int64_max_is_bound_as_decimal():
    """Regression: teradatasql packs ints as signed 64-bit and overflows on uint64.

    column_type() declares uint64 as DECIMAL(20,0), so the value must reach the
    driver as a Decimal or the write raises struct.error.
    """
    handler = TeradataPandasTypeHandler()
    frame = pd.DataFrame({"v": pd.Series([2**64 - 1], dtype="uint64")})

    assert handler.column_type("v", frame["v"]) == "DECIMAL(20,0)"
    rows = handler._rows(frame, frozenset())
    assert rows == [[Decimal(2**64 - 1)]]
    assert isinstance(rows[0][0], Decimal)


@pytest.mark.parametrize("dtype", ["uint64", "UInt64"])
def test_uint64_column_is_bound_entirely_as_decimal(dtype):
    """A batch must not mix int and Decimal parameters for one column."""
    handler = TeradataPandasTypeHandler()
    frame = pd.DataFrame({"v": pd.Series([1, 2**64 - 1], dtype=dtype)})

    rows = handler._rows(frame, frozenset())
    assert rows == [[Decimal(1)], [Decimal(2**64 - 1)]]
    assert all(type(row[0]) is Decimal for row in rows)


def test_int64_values_are_still_bound_as_plain_ints():
    """The Decimal conversion must not disturb values the driver handles natively."""
    handler = TeradataPandasTypeHandler()
    frame = pd.DataFrame({"v": pd.Series([2**63 - 1], dtype="int64")})

    rows = handler._rows(frame, frozenset())
    assert rows == [[2**63 - 1]]
    assert type(rows[0][0]) is int


def test_handle_output_infers_each_column_type_once(handler, monkeypatch):
    # String/object columns are sized by scanning their values, so column_type()
    # must be computed once per column and reused for the DDL, the row conversion
    # and the metadata.
    connection = MagicMock()
    _cursor(connection).fetchone.return_value = None
    frame = pd.DataFrame({"a": [1, 2], "s": ["x", "yy"]})
    calls = []
    original = handler.column_type
    monkeypatch.setattr(
        handler,
        "column_type",
        lambda name, series: calls.append(name) or original(name, series),
    )

    handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    assert calls == ["a", "s"]


def test_varchar_width_is_measured_in_utf16_code_units():
    # Each emoji is one code point but two UTF-16 units of a UNICODE column.
    handler = TeradataPandasTypeHandler(min_varchar_length=1)
    series = pd.Series(["\U0001f600" * 10, "abc"])
    assert handler.column_type("col", series) == "VARCHAR(40) CHARACTER SET UNICODE"
    wide = pd.Series(["\U0001f600" * 10_000])
    assert handler.column_type("col", wide) == "CLOB CHARACTER SET UNICODE"

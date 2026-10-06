"""Unit tests for the polars type handler, using a mocked Teradata connection."""

import datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
import teradatasql
from dagster._core.storage.db_io_manager import TableSlice

import dagster_teradata
from dagster_teradata import TeradataResource


pytest.importorskip("polars")

import polars as pl  # noqa: E402
from dagster_teradata.polars_type_handler import (  # noqa: E402
    DEFAULT_CHUNK_SIZE,
    TeradataPolarsIOManager,
    TeradataPolarsTypeHandler,
)


@pytest.fixture
def handler() -> TeradataPolarsTypeHandler:
    return TeradataPolarsTypeHandler()


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
    ("dtype", "expected"),
    [
        (pl.Int8, "BYTEINT"),
        (pl.Int16, "SMALLINT"),
        (pl.Int32, "INTEGER"),
        (pl.Int64, "BIGINT"),
        (pl.UInt8, "SMALLINT"),
        (pl.UInt16, "INTEGER"),
        (pl.UInt32, "BIGINT"),
        (pl.UInt64, "DECIMAL(20,0)"),
        (pl.Float32, "FLOAT"),
        (pl.Float64, "FLOAT"),
        (pl.Boolean, "BYTEINT"),
        (pl.Date, "DATE"),
        (pl.Time, "TIME(6)"),
        (pl.Binary, "BLOB"),
    ],
)
def test_column_type_mapping(handler, dtype, expected):
    series = pl.Series("col", [], dtype=dtype)
    assert handler.column_type("col", series) == expected


@pytest.mark.parametrize(
    ("dtype", "expected"),
    [
        (pl.Datetime("us"), "TIMESTAMP(6)"),
        (pl.Datetime("us", time_zone="UTC"), "TIMESTAMP(6) WITH TIME ZONE"),
        (pl.Datetime("ms"), "TIMESTAMP(6)"),
    ],
)
def test_datetime_dtype_mapping(handler, dtype, expected):
    series = pl.Series("col", [], dtype=dtype)
    assert handler.column_type("col", series) == expected


def test_decimal_dtype_keeps_precision_and_scale(handler):
    series = pl.Series("amount", ["12.345"], dtype=pl.Decimal(5, 3))
    assert handler.column_type("amount", series) == "DECIMAL(5,3)"


def test_decimal_dtype_without_declared_precision_uses_max(handler):
    series = pl.Series("amount", [Decimal("1.5")], dtype=pl.Decimal(None, 2))
    assert handler.column_type("amount", series) == "DECIMAL(38,2)"


def test_varchar_width_has_headroom_and_floor(handler):
    short = pl.Series(["ab"])
    assert handler.column_type("col", short) == "VARCHAR(256) CHARACTER SET UNICODE"

    long_value = pl.Series(["x" * 400])
    assert (
        handler.column_type("col", long_value) == "VARCHAR(800) CHARACTER SET UNICODE"
    )


def test_very_wide_strings_become_clob(handler):
    wide = pl.Series(["x" * 20_000])
    assert handler.column_type("col", wide) == "CLOB CHARACTER SET UNICODE"


def test_categorical_column_sized_from_the_values_present():
    # polars has no per-column category domain for Categorical -- the dtype's
    # categories are a shared, process-global registry -- so the column is sized
    # from the values actually in it, exactly like a String column.
    handler = TeradataPolarsTypeHandler(min_varchar_length=10)
    full = pl.Series(["short", "long_category_value"]).cast(pl.Categorical)

    assert handler.column_type("col", full) == "VARCHAR(38) CHARACTER SET UNICODE"
    # The unused long category must not influence a frame that does not contain it.
    assert (
        handler.column_type("col", full.head(1)) == "VARCHAR(10) CHARACTER SET UNICODE"
    )


def test_enum_column_sized_from_the_declared_domain():
    # Unlike Categorical, Enum's categories are a real per-column domain, so the
    # column must be sized from the full declared domain -- not just the values
    # present in this materialization -- otherwise a later frame with an
    # unchanged schema but a longer valid member than any seen so far would fail
    # against the already-created table.
    handler = TeradataPolarsTypeHandler(min_varchar_length=10)
    full = pl.Series(["short"], dtype=pl.Enum(["short", "long_category_value"]))

    assert handler.column_type("col", full) == "VARCHAR(38) CHARACTER SET UNICODE"
    # Even a frame containing only the short member must still be sized from the
    # declared domain, not the values actually present.
    assert (
        handler.column_type("col", full.head(1)) == "VARCHAR(38) CHARACTER SET UNICODE"
    )


def test_null_only_column_falls_back_to_varchar(handler):
    series = pl.Series("col", [None, None])
    assert handler.column_type("col", series) == "VARCHAR(256) CHARACTER SET UNICODE"


def test_null_only_column_becomes_clob_when_min_length_exceeds_varchar_limit():
    # An all-null column must go through the same sizing helper as any other
    # string column: emitting VARCHAR(32001) here would be invalid Teradata DDL.
    handler = TeradataPolarsTypeHandler(min_varchar_length=32_001)
    assert (
        handler.column_type("col", pl.Series("col", [None, None]))
        == "CLOB CHARACTER SET UNICODE"
    )
    # ...and stays consistent with a non-null string column at the same setting.
    assert (
        handler.column_type("col", pl.Series("col", ["a"]))
        == "CLOB CHARACTER SET UNICODE"
    )


def test_duration_raises_actionable_error(handler):
    series = pl.Series("elapsed", [datetime.timedelta(days=1)])
    with pytest.raises(ValueError, match="unsupported Duration dtype"):
        handler.column_type("elapsed", series)


@pytest.mark.parametrize(
    "values",
    [
        [[1, 2], [3]],
        [{"a": 1}, {"a": 2}],
    ],
)
def test_nested_dtypes_raise_actionable_error(handler, values):
    series = pl.Series("nested", values)
    with pytest.raises(ValueError, match="unsupported nested dtype"):
        handler.column_type("nested", series)


def test_explicit_column_types_override_inference():
    handler = TeradataPolarsTypeHandler(column_types={"id": "DECIMAL(38,0)"})
    assert handler.column_type("id", pl.Series([1], dtype=pl.Int64)) == "DECIMAL(38,0)"


def test_min_varchar_length_is_configurable():
    handler = TeradataPolarsTypeHandler(min_varchar_length=10)
    assert (
        handler.column_type("col", pl.Series(["ab"]))
        == "VARCHAR(10) CHARACTER SET UNICODE"
    )


def test_invalid_constructor_arguments():
    with pytest.raises(ValueError, match="chunk_size"):
        TeradataPolarsTypeHandler(chunk_size=0)
    with pytest.raises(ValueError, match="min_varchar_length"):
        TeradataPolarsTypeHandler(min_varchar_length=0)


# --------------------------------------------------------------------------------------
# handle_output
# --------------------------------------------------------------------------------------


def test_handle_output_creates_table_and_inserts(handler):
    connection = MagicMock()
    cursor = _cursor(connection)
    cursor.fetchone.return_value = None  # table_exists check: no matching row
    obj = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})

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
    transaction after a DDL statement)."""
    connection = MagicMock()
    cursor = _cursor(connection)
    cursor.fetchone.return_value = (1,)  # table_exists check finds a matching row
    obj = pl.DataFrame({"a": [1, 2]})

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
        MagicMock(), make_table_slice(), pl.DataFrame({"a": [1]}), connection
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
            MagicMock(), make_table_slice(), pl.DataFrame({"a": [1]}), connection
        )


def test_handle_output_chunks_large_frames():
    handler = TeradataPolarsTypeHandler(chunk_size=2)
    connection = MagicMock()
    obj = pl.DataFrame({"a": range(5)})

    handler.handle_output(MagicMock(), make_table_slice(), obj, connection)

    batches = [call.args[1] for call in _cursor(connection).executemany.call_args_list]
    assert [len(batch) for batch in batches] == [2, 2, 1]
    assert [row[0] for batch in batches for row in batch] == [0, 1, 2, 3, 4]


def test_handle_output_converts_nan_and_nulls(handler):
    connection = MagicMock()
    obj = pl.DataFrame(
        {
            "n": [1.0, float("nan")],
            "s": ["a", None],
            "ts": [datetime.datetime(2024, 1, 1, 10, 0), None],
        }
    )

    handler.handle_output(MagicMock(), make_table_slice(), obj, connection)

    rows = _cursor(connection).executemany.call_args.args[1]
    assert rows[0] == [1.0, "a", datetime.datetime(2024, 1, 1, 10, 0)]
    # NaN cannot be stored in a Teradata FLOAT column, so it is written as NULL.
    assert rows[1] == [None, None, None]


def test_handle_output_stringifies_unsupported_objects_in_varchar_columns(handler):
    # An object-typed column falls back to VARCHAR DDL, so the value sent to the
    # driver must be a string, not the original unsupported object.
    connection = MagicMock()
    obj = pl.DataFrame({"payload": pl.Series([{"a": 1}, None], dtype=pl.Object)})

    handler.handle_output(MagicMock(), make_table_slice(), obj, connection)

    rows = _cursor(connection).executemany.call_args.args[1]
    assert rows == [["{'a': 1}"], [None]]
    assert all(value is None or isinstance(value, str) for row in rows for value in row)


@pytest.mark.parametrize("override", ["CHAR(20)", "CHARACTER VARYING(20)"])
def test_handle_output_stringifies_objects_for_character_overrides(override):
    handler = TeradataPolarsTypeHandler(column_types={"payload": override})
    connection = MagicMock()
    obj = pl.DataFrame({"payload": pl.Series([{"a": 1}, None], dtype=pl.Object)})

    handler.handle_output(MagicMock(), make_table_slice(), obj, connection)

    rows = _cursor(connection).executemany.call_args.args[1]
    assert rows == [["{'a': 1}"], [None]]


def test_handle_output_leaves_non_string_columns_unconverted(handler):
    # Columns inferred as a non-string type (e.g. FLOAT here) must not be stringified.
    connection = MagicMock()
    obj = pl.DataFrame({"n": [1.5, 2.5]})

    handler.handle_output(MagicMock(), make_table_slice(), obj, connection)

    rows = _cursor(connection).executemany.call_args.args[1]
    assert rows == [[1.5], [2.5]]


def test_handle_output_skips_insert_for_empty_frame(handler):
    connection = MagicMock()
    obj = pl.DataFrame({"a": pl.Series([], dtype=pl.Int64)})

    metadata = handler.handle_output(MagicMock(), make_table_slice(), obj, connection)

    _cursor(connection).executemany.assert_not_called()
    assert metadata["row_count"] == 0


def test_handle_output_rejects_frame_without_columns(handler):
    with pytest.raises(ValueError, match="no columns"):
        handler.handle_output(
            MagicMock(), make_table_slice(), pl.DataFrame(), MagicMock()
        )


def test_handle_output_rejects_case_insensitive_duplicate_column_names(handler):
    # Teradata identifiers are case-insensitive, so "A" and "a" collide even though
    # polars treats them as distinct column labels.
    obj = pl.DataFrame({"A": [1], "a": [2]})
    with pytest.raises(ValueError, match="only differ by letter case"):
        handler.handle_output(MagicMock(), make_table_slice(), obj, MagicMock())


def test_handle_output_rejects_non_dataframe(handler):
    with pytest.raises(TypeError, match="only store polars DataFrames"):
        handler.handle_output(MagicMock(), make_table_slice(), [1, 2], MagicMock())


def test_handle_output_returns_column_schema_metadata(handler):
    connection = MagicMock()
    obj = pl.DataFrame({"a": [1], "b": ["x"]})

    metadata = handler.handle_output(MagicMock(), make_table_slice(), obj, connection)

    schema = metadata["dagster/column_schema"]
    assert [(c.name, c.type) for c in schema.schema.columns] == [
        ("a", "BIGINT"),
        ("b", "VARCHAR(256) CHARACTER SET UNICODE"),
    ]


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
    assert result.equals(pl.DataFrame({"a": [1, 2], "b": ["x", "y"]}))


def test_load_input_returns_empty_frame_with_columns(handler):
    connection = MagicMock()
    cursor = _cursor(connection)
    cursor.description = [("a", None)]
    cursor.fetchall.return_value = []

    result = handler.load_input(MagicMock(), make_table_slice(), connection)

    assert result.is_empty()
    assert result.columns == ["a"]


def test_load_input_types_an_empty_frame_from_cursor_description(handler):
    # With no rows there is nothing to infer from, so the dtypes come from the
    # Python types teradatasql reports as each column's type_code.
    connection = MagicMock()
    cursor = _cursor(connection)
    cursor.description = [
        ("i", int, None, None, None, None, True),
        ("f", float, None, None, None, None, True),
        ("s", str, None, None, None, None, True),
        ("d", Decimal, None, None, 10, 2, True),
        ("dt", datetime.date, None, None, None, None, True),
        ("ts", datetime.datetime, None, None, None, None, True),
        ("t", datetime.time, None, None, None, None, True),
        ("b", bytes, None, None, None, None, True),
        ("u", object, None, None, None, None, True),
    ]
    cursor.fetchall.return_value = []

    result = handler.load_input(MagicMock(), make_table_slice(), connection)

    assert result.is_empty()
    assert result.schema == pl.Schema(
        {
            "i": pl.Int64,
            "f": pl.Float64,
            "s": pl.String,
            "d": pl.Decimal(10, 2),
            "dt": pl.Date,
            "ts": pl.Datetime("us"),
            "t": pl.Time,
            "b": pl.Binary,
            "u": pl.Null,
        }
    )


def test_handle_output_infers_each_column_type_once(handler, monkeypatch):
    # String columns are sized with a full scan, so column_type() must be computed
    # once per column and reused for the DDL, the row conversion and the metadata.
    connection = MagicMock()
    _cursor(connection).fetchone.return_value = None
    frame = pl.DataFrame({"a": [1, 2], "s": ["x", "yy"]})
    calls = []
    original = handler.column_type
    monkeypatch.setattr(
        handler,
        "column_type",
        lambda name, series: calls.append(name) or original(name, series),
    )

    handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    assert calls == ["a", "s"]


def test_load_input_infers_dtype_from_all_rows_not_just_the_first_100(handler):
    """A nullable column whose first 100 values are NULL must not be inferred as
    Null: polars would then fail with "could not append value" on the first
    non-NULL row further down."""
    connection = MagicMock()
    cursor = _cursor(connection)
    cursor.description = [("a", None)]
    cursor.fetchall.return_value = [[None] for _ in range(100)] + [[42]]

    result = handler.load_input(MagicMock(), make_table_slice(), connection)

    assert result["a"].dtype == pl.Int64
    assert result["a"].to_list()[-1] == 42


def test_load_input_types_all_null_columns_from_cursor_description(handler):
    # Value inference yields Null for a column that is NULL in every row; the
    # database type must be applied so a downstream write does not remap it to
    # VARCHAR. Columns with values keep their inferred dtypes.
    connection = MagicMock()
    cursor = _cursor(connection)
    cursor.description = [
        ("i", int, None, None, None, None, True),
        ("d", Decimal, None, None, 10, 2, True),
        ("dt", datetime.date, None, None, None, None, True),
        ("u", object, None, None, None, None, True),
        ("s", str, None, None, None, None, True),
    ]
    cursor.fetchall.return_value = [
        [None, None, None, None, "x"],
        [None, None, None, None, "y"],
    ]

    result = handler.load_input(MagicMock(), make_table_slice(), connection)

    assert result.schema == pl.Schema(
        {
            "i": pl.Int64,
            "d": pl.Decimal(10, 2),
            "dt": pl.Date,
            "u": pl.Null,
            "s": pl.String,
        }
    )
    assert result["i"].null_count() == 2
    assert result["s"].to_list() == ["x", "y"]


def test_supported_types(handler):
    assert handler.supported_types == [pl.DataFrame]


_ZONED_DESCRIPTION = [
    ("naive", datetime.datetime, None, None, None, None, True),
    ("zoned", datetime.datetime, None, None, None, None, True),
]


@pytest.mark.parametrize("rows", [[], [[None, None], [None, None]]])
def test_load_input_types_valueless_zoned_timestamps_from_catalog(handler, rows):
    # cursor.description cannot tell TIMESTAMP from TIMESTAMP WITH TIME ZONE, so
    # a column with no values to infer from is looked up in DBC.ColumnsV.
    connection = MagicMock()
    cursor = _cursor(connection)
    cursor.description = _ZONED_DESCRIPTION
    cursor.fetchall.side_effect = [rows, [("ZONED   ",)]]

    result = handler.load_input(MagicMock(), make_table_slice(), connection)

    assert result.height == len(rows)
    assert result.schema == pl.Schema(
        {"naive": pl.Datetime("us"), "zoned": pl.Datetime("us", "UTC")}
    )
    catalog_sql, catalog_params = cursor.execute.call_args_list[1].args
    assert "ColumnType = 'SZ'" in catalog_sql
    assert catalog_params == ["my_db", "my_table"]


def test_load_input_keeps_naive_fallback_when_catalog_unreadable(handler):
    connection = MagicMock()
    cursor = _cursor(connection)
    cursor.description = _ZONED_DESCRIPTION
    cursor.fetchall.return_value = []
    cursor.execute.side_effect = [None, teradatasql.DatabaseError("no access")]

    result = handler.load_input(MagicMock(), make_table_slice(), connection)

    assert result.schema == pl.Schema(
        {"naive": pl.Datetime("us"), "zoned": pl.Datetime("us")}
    )


def test_load_input_skips_catalog_when_timestamps_have_values(handler):
    connection = MagicMock()
    cursor = _cursor(connection)
    cursor.description = _ZONED_DESCRIPTION
    value = datetime.datetime(2024, 1, 2, tzinfo=datetime.timezone.utc)
    cursor.fetchall.return_value = [[value.replace(tzinfo=None), value]]

    result = handler.load_input(MagicMock(), make_table_slice(), connection)

    assert cursor.execute.call_count == 1
    assert result.schema["zoned"] == pl.Datetime("us", "UTC")


# --------------------------------------------------------------------------------------
# I/O manager wiring
# --------------------------------------------------------------------------------------


def test_io_manager_passes_configuration_to_handler():
    io_manager = TeradataPolarsIOManager(
        teradata=TeradataResource(host="localhost", user="dbc", password="dbc"),
        chunk_size=17,
        min_varchar_length=42,
        column_types={"amount": "DECIMAL(18,4)"},
    )
    (handler,) = io_manager.type_handlers()
    assert handler.chunk_size == 17
    assert handler.min_varchar_length == 42
    assert handler.column_types == {"amount": "DECIMAL(18,4)"}
    assert io_manager.default_load_type() is pl.DataFrame


def test_io_manager_column_types_override_inference():
    """The documented escape hatch must reach the column type mapping."""
    io_manager = TeradataPolarsIOManager(
        teradata=TeradataResource(host="localhost", user="dbc", password="dbc"),
        column_types={"amount": "DECIMAL(18,4)"},
    )
    (handler,) = io_manager.type_handlers()
    frame = pl.DataFrame({"amount": [1.5], "other": [1.5]})
    assert handler.column_type("amount", frame["amount"]) == "DECIMAL(18,4)"
    assert handler.column_type("other", frame["other"]) == "FLOAT"


def test_io_manager_defaults():
    io_manager = TeradataPolarsIOManager(
        teradata=TeradataResource(host="localhost", user="dbc", password="dbc"),
    )
    (handler,) = io_manager.type_handlers()
    assert handler.chunk_size == DEFAULT_CHUNK_SIZE


def test_lazy_exports_available_from_package_root():
    assert dagster_teradata.TeradataPolarsIOManager is TeradataPolarsIOManager
    assert dagster_teradata.TeradataPolarsTypeHandler is TeradataPolarsTypeHandler
    assert "TeradataPolarsIOManager" in dir(dagster_teradata)


def test_categorical_column_is_not_widened_by_unrelated_frames():
    """Regression: sizing must use this column's categories, not the global registry.

    polars' Categorical dtype carries a handle to a process-global string registry
    shared by every categorical Series in the process. Sizing from it let a wholly
    unrelated frame widen this column -- or push it past the CLOB cutoff -- and the
    table is only created once, so the wrong type would be permanent.
    """
    handler = TeradataPolarsTypeHandler()
    frame = pl.DataFrame({"c": pl.Series(["ab"], dtype=pl.Categorical)})
    before = handler.column_type("c", frame["c"])

    # Intern a very long category from an unrelated frame into the same registry.
    pl.DataFrame({"unrelated": pl.Series(["x" * 20_000], dtype=pl.Categorical)})

    assert handler.column_type("c", frame["c"]) == before
    assert before == "VARCHAR(256) CHARACTER SET UNICODE"


def test_uint64_above_int64_max_is_bound_as_decimal():
    """Regression: teradatasql packs ints as signed 64-bit and overflows on uint64.

    column_type() declares UInt64 as DECIMAL(20,0), so the value must reach the
    driver as a Decimal or the write raises struct.error.
    """
    handler = TeradataPolarsTypeHandler()
    frame = pl.DataFrame({"v": pl.Series([2**64 - 1], dtype=pl.UInt64)})

    assert handler.column_type("v", frame["v"]) == "DECIMAL(20,0)"
    rows = handler._rows(frame, frozenset())
    assert rows == [[Decimal(2**64 - 1)]]
    assert isinstance(rows[0][0], Decimal)


def test_uint64_column_is_bound_entirely_as_decimal():
    """A batch must not mix int and Decimal parameters for one column."""
    handler = TeradataPolarsTypeHandler()
    frame = pl.DataFrame({"v": pl.Series([1, None, 2**64 - 1], dtype=pl.UInt64)})

    rows = handler._rows(frame, frozenset())
    assert rows == [[Decimal(1)], [None], [Decimal(2**64 - 1)]]
    assert type(rows[0][0]) is Decimal


def test_int64_values_are_still_bound_as_plain_ints():
    """The Decimal conversion must not disturb values the driver handles natively."""
    handler = TeradataPolarsTypeHandler()
    frame = pl.DataFrame({"v": pl.Series([2**63 - 1], dtype=pl.Int64)})

    rows = handler._rows(frame, frozenset())
    assert rows == [[2**63 - 1]]
    assert type(rows[0][0]) is int


@pytest.mark.parametrize("dtype", [pl.String, pl.Object])
def test_varchar_width_is_measured_in_utf16_code_units(dtype):
    # Each emoji is one code point but two UTF-16 units of a UNICODE column.
    handler = TeradataPolarsTypeHandler(min_varchar_length=1)
    series = pl.Series("col", ["\U0001f600" * 10, "ab\u00e9"], dtype=dtype)
    assert handler.column_type("col", series) == "VARCHAR(40) CHARACTER SET UNICODE"
    wide = pl.Series("col", ["\U0001f600" * 10_000], dtype=dtype)
    assert handler.column_type("col", wide) == "CLOB CHARACTER SET UNICODE"

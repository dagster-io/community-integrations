"""polars type handler for :py:class:`~dagster_teradata.TeradataIOManager`.

This module imports polars at module scope. It is deliberately *not* imported by
``dagster_teradata/__init__.py`` at import time so that polars stays an optional
dependency; the package exposes the names below lazily instead.

Install the optional dependency with::

    pip install "dagster-teradata[polars]"
"""

import datetime
import math
from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import Any

import polars as pl
from dagster import InputContext, MetadataValue, OutputContext, TableColumn, TableSchema
from dagster._core.storage.db_io_manager import DbTypeHandler, TableSlice
from pydantic import Field

from dagster_teradata._catalog import zoned_timestamp_columns
from dagster_teradata._type_handler_base import (
    MAX_VARCHAR_LENGTH as _MAX_VARCHAR_LENGTH,
    TeradataTableTypeHandler,
    bindable_int,
    case_insensitive_duplicates,
    is_string_type,
    utf16_length,
)
from dagster_teradata.io_manager import (
    TeradataDbClient,
    TeradataIOManager,
    build_teradata_io_manager,
)

# Rows sent to the driver per executemany call.
DEFAULT_CHUNK_SIZE = 5_000

# A character outside the BMP: one code point, but two UTF-16 code units.
_SUPPLEMENTARY_CHARACTER = r"[\x{10000}-\x{10FFFF}]"

_SIGNED_INTEGER_TYPES = {
    pl.Int8: "BYTEINT",
    pl.Int16: "SMALLINT",
    pl.Int32: "INTEGER",
    pl.Int64: "BIGINT",
}

# Teradata integer types are all signed, so an unsigned dtype's max value (e.g.
# UInt8's 255) can exceed the same-width signed type's range. Use the next widest
# signed type; UInt64 needs a DECIMAL since even BIGINT is too narrow for its full
# range.
_UNSIGNED_INTEGER_TYPES = {
    pl.UInt8: "SMALLINT",
    pl.UInt16: "INTEGER",
    pl.UInt32: "BIGINT",
    pl.UInt64: "DECIMAL(20,0)",
}


# Python types teradatasql reports as cursor.description type codes, mapped to the
# polars dtypes load_input() would otherwise infer from the values themselves.
_DESCRIPTION_DTYPES: dict[Any, Any] = {
    int: pl.Int64,
    float: pl.Float64,
    str: pl.String,
    bytes: pl.Binary,
    datetime.date: pl.Date,
    datetime.datetime: pl.Datetime("us"),
    datetime.time: pl.Time,
}


class TeradataPolarsTypeHandler(TeradataTableTypeHandler[pl.DataFrame]):
    """Stores and loads polars DataFrames as Teradata tables.

    The table is created on first materialization from the DataFrame's dtypes and
    reused afterwards, so a later change in DataFrame shape does not alter an
    existing table. Use ``column_types`` to pin a column to an exact Teradata type
    when the inferred one is not what you want.

    Args:
        chunk_size (int): Rows sent per ``executemany`` batch. Defaults to 5000.
        min_varchar_length (int): Floor applied to inferred ``VARCHAR`` widths, so
            that columns are not sized so tightly that a later, longer value fails
            to insert. Defaults to 256.
        column_types (Mapping[str, str] | None): Explicit Teradata column types,
            keyed by column name. Overrides inference entirely for those columns.

    Example:
        .. code-block:: python

            from dagster_teradata import TeradataIOManager, TeradataPolarsTypeHandler

            class MyIOManager(TeradataIOManager):
                def type_handlers(self):
                    return [TeradataPolarsTypeHandler(chunk_size=20_000)]
    """

    def __init__(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        min_varchar_length: int = 256,
        column_types: Mapping[str, str] | None = None,
    ):
        if chunk_size < 1:
            raise ValueError(f"chunk_size must be at least 1, got {chunk_size}.")
        if min_varchar_length < 1:
            raise ValueError(
                f"min_varchar_length must be at least 1, got {min_varchar_length}."
            )
        self.chunk_size = chunk_size
        self.min_varchar_length = min_varchar_length
        self.column_types = dict(column_types or {})

    def _string_type(self, series: pl.Series) -> str:
        non_null = series.drop_nulls()
        # Measured in UTF-16 code units, the unit Teradata sizes UNICODE columns
        # in: a supplementary character (outside the BMP) takes two.
        if not len(non_null):
            max_length = 0
        elif series.dtype == pl.Object:
            # Object columns cannot be cast to String by polars; measure the values'
            # string representations directly instead.
            max_length = max(utf16_length(str(value)) for value in non_null)
        else:
            strings = non_null.cast(pl.String).str
            max_length = int(
                (
                    strings.len_chars()
                    + strings.count_matches(_SUPPLEMENTARY_CHARACTER)
                ).max()
            )
        # Double the observed width, with a floor, to leave headroom: the table is
        # only created once, so a tight bound would break later materializations.
        length = max(max_length * 2, self.min_varchar_length)
        if length > _MAX_VARCHAR_LENGTH:
            return "CLOB CHARACTER SET UNICODE"
        # Explicit so the column never inherits a LATIN user default.
        return f"VARCHAR({length}) CHARACTER SET UNICODE"

    def column_type(self, name: str, series: pl.Series) -> str:
        """Return the Teradata column type used to create ``name``."""
        if name in self.column_types:
            return self.column_types[name]

        dtype = series.dtype
        if isinstance(dtype, pl.Categorical):
            # Size from the values actually present, exactly like a String column.
            # polars offers no per-column category domain for Categorical: both
            # dtype.categories and Series.cat.get_categories() return the (by
            # default process-global) registry shared by every categorical Series
            # in the process, so sizing from either lets an unrelated frame widen
            # this column -- or push it past the CLOB cutoff -- permanently, since
            # the table is created only once. pl.Enum does carry a real per-column
            # domain and is sized from it explicitly below.
            return self._string_type(series.cast(pl.String))
        if dtype == pl.Boolean:
            return "BYTEINT"
        if dtype in _SIGNED_INTEGER_TYPES:
            return _SIGNED_INTEGER_TYPES[dtype]
        if dtype in _UNSIGNED_INTEGER_TYPES:
            return _UNSIGNED_INTEGER_TYPES[dtype]
        if dtype in (pl.Float32, pl.Float64):
            return "FLOAT"
        if isinstance(dtype, pl.Decimal):
            # polars itself enforces precision <= 38 (Teradata's maximum DECIMAL
            # precision), so no upper-bound check is needed here.
            precision = dtype.precision if dtype.precision is not None else 38
            scale = dtype.scale if dtype.scale is not None else 0
            return f"DECIMAL({precision},{scale})"
        if isinstance(dtype, pl.Datetime):
            if dtype.time_zone is not None:
                return "TIMESTAMP(6) WITH TIME ZONE"
            return "TIMESTAMP(6)"
        if dtype == pl.Date:
            return "DATE"
        if dtype == pl.Time:
            return "TIME(6)"
        if isinstance(dtype, pl.Duration):
            raise ValueError(
                f"Column '{name}' has an unsupported Duration dtype. Convert it to a "
                "number of seconds or a string before storing it in Teradata."
            )
        if dtype == pl.Binary:
            return "BLOB"
        if isinstance(dtype, pl.Enum):
            # Unlike Categorical, Enum's categories are a real per-column domain
            # (not a process-global registry), so size from the declared domain
            # rather than only the values present in this materialization --
            # otherwise a later, unchanged-schema frame with a longer valid member
            # than any seen so far would fail against the already-created table.
            return self._string_type(dtype.categories)
        if dtype == pl.String:
            return self._string_type(series)
        if dtype == pl.Null:
            # An all-null column has no observable width, so this yields exactly
            # VARCHAR(min_varchar_length) -- but it must go through the same helper
            # as any other string column so a min_varchar_length above Teradata's
            # VARCHAR limit falls back to CLOB instead of emitting invalid DDL.
            return self._string_type(series)
        if isinstance(dtype, (pl.List, pl.Array, pl.Struct)):
            raise ValueError(
                f"Column '{name}' has an unsupported nested dtype ({dtype}). Flatten "
                "or serialize it (for example to a JSON string) before storing it "
                "in Teradata."
            )
        return self._string_type(series)

    def column_types_for(self, obj: pl.DataFrame) -> dict[str, str]:
        """Teradata type of every column, computed once per materialization.

        String-like columns are sized with a full scan of their values (a
        Python-level one for ``pl.Object``), so this is evaluated a single time and
        reused for the DDL, the row conversion and the metadata rather than once
        for each.
        """
        return {name: self.column_type(name, obj[name]) for name in obj.columns}

    @staticmethod
    def _to_python(value: Any) -> Any:
        """Convert a polars scalar into something the driver accepts."""
        # polars keeps float('nan') distinct from null; Teradata FLOAT cannot hold
        # NaN, so treat it as missing, matching the pandas handler's behaviour.
        if isinstance(value, float) and math.isnan(value):
            return None
        return bindable_int(value)

    def _rows(
        self, obj: pl.DataFrame, string_columns: frozenset[str]
    ) -> list[list[Any]]:
        # Values destined for a VARCHAR/CLOB column must be converted to strings here:
        # the column can hold values the driver cannot bind directly, even though the
        # DDL declares a string type.
        columns = obj.columns
        # Bind every value of a UInt64 (DECIMAL(20,0)) column as Decimal, not only
        # those above 2**63-1, so a batch never mixes int and Decimal parameters
        # for one column.
        decimal_columns = {
            name for name, dtype in obj.schema.items() if dtype == pl.UInt64
        }
        rows = []
        for row in obj.rows():
            converted_row = []
            for name, value in zip(columns, row):
                value = self._to_python(value)
                if value is not None and name in string_columns:
                    value = str(value)
                elif value is not None and name in decimal_columns:
                    value = Decimal(value)
                converted_row.append(value)
            rows.append(converted_row)
        return rows

    def handle_output(
        self,
        context: OutputContext,
        table_slice: TableSlice,
        obj: pl.DataFrame,
        connection: Any,
    ) -> Mapping[str, Any]:
        if not isinstance(obj, pl.DataFrame):
            raise TypeError(
                f"TeradataPolarsTypeHandler can only store polars DataFrames, got "
                f"{type(obj)}."
            )
        if not obj.columns:
            raise ValueError(
                "Cannot store a DataFrame with no columns: Teradata tables require at "
                "least one column."
            )
        # polars forbids duplicate column names at construction, but Teradata
        # identifiers are case-insensitive, so labels such as "A" and "a" collide.
        case_insensitive = case_insensitive_duplicates(obj.columns)
        if case_insensitive:
            raise ValueError(
                "Cannot store a DataFrame with column names that only differ by "
                f"letter case: {case_insensitive}. Teradata identifiers "
                "are case-insensitive, so these columns would produce duplicate "
                "identifiers in the generated table and INSERT statements."
            )

        column_types = self.column_types_for(obj)
        with connection.cursor() as cursor:
            self._create_table_if_absent(cursor, connection, table_slice, column_types)

            if len(obj):
                # Determined once from the full DataFrame so every chunk agrees with
                # the types used to create the table, rather than re-inferring per
                # chunk.
                string_columns = frozenset(
                    name
                    for name, td_type in column_types.items()
                    if is_string_type(td_type)
                )
                self._insert_rows(
                    cursor,
                    table_slice,
                    column_types,
                    (
                        self._rows(obj.slice(start, self.chunk_size), string_columns)
                        for start in range(0, len(obj), self.chunk_size)
                    ),
                )

        return {
            "row_count": obj.shape[0],
            "dagster/column_schema": MetadataValue.table_schema(
                TableSchema(
                    columns=[
                        TableColumn(name=name, type=td_type)
                        for name, td_type in column_types.items()
                    ]
                )
            ),
        }

    @staticmethod
    def _polars_dtype(description: Sequence[Any]) -> Any:
        """polars dtype for a ``cursor.description`` entry, or ``pl.Null``.

        teradatasql reports each column's Python type as its ``type_code``; this is
        used to type empty results and columns that are NULL in every row, where
        there are no values to infer from.
        """
        type_code = description[1]
        precision = description[4] if len(description) > 4 else None
        scale = description[5] if len(description) > 5 else None
        if type_code is Decimal:
            if isinstance(precision, int) and isinstance(scale, int):
                return pl.Decimal(precision, scale)
            return pl.Decimal(None, scale if isinstance(scale, int) else 0)
        return _DESCRIPTION_DTYPES.get(type_code, pl.Null)

    def load_input(
        self, context: InputContext, table_slice: TableSlice, connection: Any
    ) -> pl.DataFrame:
        with connection.cursor() as cursor:
            # get_select_statement applies both column selection and the partition
            # WHERE clause, so partitioned inputs only read their own rows.
            cursor.execute(TeradataDbClient.get_select_statement(table_slice))
            description = list(cursor.description)
            rows = cursor.fetchall()
            column_names = [entry[0] for entry in description]
            if rows:
                # infer_schema_length=None scans every row rather than polars'
                # default of 100. A nullable column whose first 100 values happen to
                # be NULL would otherwise be inferred as Null and then fail with
                # "could not append value" on the first non-NULL row further down.
                # The rows are already fully materialized by fetchall(), so this
                # costs no extra I/O.
                frame = pl.DataFrame(
                    rows, schema=column_names, orient="row", infer_schema_length=None
                )
            else:
                frame = pl.DataFrame(schema={name: pl.Null for name in column_names})
            # A column with no values to infer from (an empty result, or NULL in
            # every row) still has a database type; apply it so a downstream write
            # does not fall back to VARCHAR. Columns with values keep their dtype.
            untyped = [
                entry for entry in description if frame.schema[entry[0]] == pl.Null
            ]
            zoned = (
                zoned_timestamp_columns(cursor, table_slice)
                if any(entry[1] is datetime.datetime for entry in untyped)
                else None
            )
        casts = {}
        for entry in untyped:
            dtype = self._polars_dtype(entry)
            if dtype == pl.Datetime("us") and zoned and entry[0].upper() in zoned:
                # teradatasql returns zoned values as aware datetimes, which polars
                # infers as UTC; match that so the column's type is stable.
                dtype = pl.Datetime("us", "UTC")
            if dtype != pl.Null:
                casts[entry[0]] = dtype
        return frame.cast(casts) if casts else frame

    @property
    def supported_types(self) -> Sequence[type]:
        return [pl.DataFrame]


class TeradataPolarsIOManager(TeradataIOManager):
    """An I/O manager that stores polars DataFrames as Teradata tables.

    Example:
        .. code-block:: python

            import polars as pl
            from dagster import Definitions, EnvVar, asset
            from dagster_teradata import TeradataPolarsIOManager, TeradataResource

            @asset(key_prefix=["analytics"])
            def customers() -> pl.DataFrame:
                return pl.DataFrame({"id": [1, 2]})

            defs = Definitions(
                assets=[customers],
                resources={
                    "io_manager": TeradataPolarsIOManager(
                        teradata=TeradataResource(
                            host=EnvVar("TERADATA_HOST"),
                            user=EnvVar("TERADATA_USER"),
                            password=EnvVar("TERADATA_PASSWORD"),
                            database=EnvVar("TERADATA_DATABASE"),
                        ),
                    )
                },
            )
    """

    chunk_size: int = Field(
        default=DEFAULT_CHUNK_SIZE,
        ge=1,
        description="Number of rows written per batch when inserting a DataFrame.",
    )
    min_varchar_length: int = Field(
        default=256,
        ge=1,
        description=(
            "Floor applied to inferred VARCHAR widths when a table is created, so "
            "columns keep headroom for longer values in later materializations."
        ),
    )
    column_types: dict[str, str] | None = Field(
        default=None,
        description=(
            "Explicit Teradata column types keyed by column name, e.g. "
            '{"amount": "DECIMAL(18,4)"}. Overrides inference for those columns and '
            "is the escape hatch for types the mapping does not infer."
        ),
    )

    def type_handlers(self) -> Sequence[DbTypeHandler]:
        return [
            TeradataPolarsTypeHandler(
                chunk_size=self.chunk_size,
                min_varchar_length=self.min_varchar_length,
                column_types=self.column_types,
            )
        ]

    @staticmethod
    def default_load_type() -> type | None:
        return pl.DataFrame


teradata_polars_io_manager = build_teradata_io_manager(
    [TeradataPolarsTypeHandler()], default_load_type=pl.DataFrame
)
"""Legacy ``@io_manager``-style equivalent of :py:class:`TeradataPolarsIOManager`."""

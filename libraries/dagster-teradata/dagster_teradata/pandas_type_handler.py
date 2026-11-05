"""pandas type handler for :py:class:`~dagster_teradata.TeradataIOManager`.

This module imports pandas at module scope. It is deliberately *not* imported by
``dagster_teradata/__init__.py`` at import time so that pandas stays an optional
dependency; the package exposes the names below lazily instead.

Install the optional dependency with::

    pip install "dagster-teradata[pandas]"
"""

from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import Any

import numpy as np
import pandas as pd
from dagster import InputContext, MetadataValue, OutputContext, TableColumn, TableSchema
from dagster._core.storage.db_io_manager import DbTypeHandler, TableSlice
from pydantic import Field

from dagster_teradata._type_handler_base import (
    MAX_DECIMAL_PRECISION as _MAX_DECIMAL_PRECISION,
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


def _integer_type(dtype: Any) -> str:
    """Map an integer dtype onto the narrowest Teradata type that fits it."""
    itemsize = getattr(dtype, "itemsize", 8)
    if getattr(dtype, "kind", None) == "u":
        # Teradata integer types are all signed, so an unsigned dtype's max value
        # (e.g. uint8's 255) can exceed the same-width signed type's range. Use the
        # next widest signed type; uint64 needs a DECIMAL since even BIGINT is too
        # narrow for its full range.
        return {1: "SMALLINT", 2: "INTEGER", 4: "BIGINT"}.get(itemsize, "DECIMAL(20,0)")
    return {1: "BYTEINT", 2: "SMALLINT", 4: "INTEGER"}.get(itemsize, "BIGINT")


def _uint64_positions(obj: pd.DataFrame) -> frozenset[int]:
    """Positions of the unsigned 64-bit (``DECIMAL(20,0)``) columns of ``obj``.

    Every value of such a column is bound as ``Decimal``, not only those above
    2**63-1, so a batch never mixes int and Decimal parameters for one column.
    """
    return frozenset(
        position
        for position, dtype in enumerate(obj.dtypes)
        if pd.api.types.is_unsigned_integer_dtype(dtype)
        and getattr(dtype, "itemsize", 8) == 8
    )


def _decimal_type(name: str, values: Sequence[Decimal]) -> str:
    """Derive a DECIMAL type wide enough for every supplied value."""
    max_integer_digits = 0
    max_scale = 0
    for value in values:
        _sign, digits, exponent = value.as_tuple()
        if not isinstance(exponent, int):
            # Infinity or NaN carry a string exponent and have no fixed precision.
            return "FLOAT"
        value_scale = max(-exponent, 0)
        # Digits to the left of the decimal point; a positive exponent shifts the
        # decimal point right past the stored digits (e.g. 1.5E+3 == 1500), and a
        # negative exponent larger than len(digits) means the value is < 1.
        integer_digits = max(len(digits) + exponent, 0)
        max_integer_digits = max(max_integer_digits, integer_digits)
        max_scale = max(max_scale, value_scale)
    precision = max(max_integer_digits + max_scale, 1)
    if precision > _MAX_DECIMAL_PRECISION:
        # Clamping the precision instead would silently produce a DECIMAL too narrow
        # to hold the value it was sized for (e.g. Decimal("1E+38") needs 39 integer
        # digits, but Teradata's DECIMAL tops out at 38).
        raise ValueError(
            f"Column '{name}' has a Decimal value that needs {precision} digits of "
            f"precision, which exceeds Teradata's maximum DECIMAL precision of "
            f"{_MAX_DECIMAL_PRECISION}. Round the value or store it as a string."
        )
    scale = min(max_scale, precision)
    return f"DECIMAL({precision},{scale})"


class TeradataPandasTypeHandler(TeradataTableTypeHandler[pd.DataFrame]):
    """Stores and loads pandas DataFrames as Teradata tables.

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

            from dagster_teradata import TeradataIOManager, TeradataPandasTypeHandler

            class MyIOManager(TeradataIOManager):
                def type_handlers(self):
                    return [TeradataPandasTypeHandler(chunk_size=20_000)]
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

    def _string_type(self, series: pd.Series) -> str:
        non_null = series.dropna()
        # UTF-16 code units, the unit Teradata sizes UNICODE columns in.
        max_length = (
            int(non_null.astype(str).map(utf16_length).max()) if len(non_null) else 0
        )
        # Double the observed width, with a floor, to leave headroom: the table is
        # only created once, so a tight bound would break later materializations.
        length = max(max_length * 2, self.min_varchar_length)
        if length > _MAX_VARCHAR_LENGTH:
            return "CLOB CHARACTER SET UNICODE"
        # Explicit so the column never inherits a LATIN user default.
        return f"VARCHAR({length}) CHARACTER SET UNICODE"

    def _object_type(self, name: str, series: pd.Series) -> str:
        """Infer a type for an ``object`` column from the values it actually holds."""
        import datetime

        non_null = series.dropna()
        if not len(non_null):
            # Through the shared helper so an oversized floor falls back to CLOB.
            return self._string_type(series)

        values = list(non_null)
        if all(isinstance(value, Decimal) for value in values):
            return _decimal_type(name, values)
        if all(isinstance(value, bool) for value in values):
            return "BYTEINT"
        if all(
            isinstance(value, datetime.date)
            and not isinstance(value, datetime.datetime)
            for value in values
        ):
            return "DATE"
        if all(isinstance(value, datetime.time) for value in values):
            return "TIME(6)"
        if all(isinstance(value, (bytes, bytearray)) for value in values):
            return "BLOB"
        return self._string_type(series)

    def column_type(self, name: str, series: pd.Series) -> str:
        """Return the Teradata column type used to create ``name``."""
        if name in self.column_types:
            return self.column_types[name]

        dtype = series.dtype
        if isinstance(dtype, pd.CategoricalDtype):
            # Size from the full declared category domain, not just the values
            # present in this materialization: a later batch can contain a longer,
            # already-declared category that this frame happens not to use.
            categories = pd.Series(dtype.categories, dtype=object)
            return self._string_type(categories)
        if pd.api.types.is_bool_dtype(dtype):
            return "BYTEINT"
        if pd.api.types.is_integer_dtype(dtype):
            return _integer_type(dtype)
        if pd.api.types.is_float_dtype(dtype):
            return "FLOAT"
        if isinstance(dtype, pd.DatetimeTZDtype):
            return "TIMESTAMP(6) WITH TIME ZONE"
        if pd.api.types.is_datetime64_any_dtype(dtype):
            return "TIMESTAMP(6)"
        if pd.api.types.is_timedelta64_dtype(dtype):
            raise ValueError(
                f"Column '{name}' has an unsupported timedelta dtype. Convert it to a "
                "number of seconds or a string before storing it in Teradata."
            )
        if pd.api.types.is_object_dtype(dtype):
            # Must be checked before is_string_dtype, which reports True for object
            # columns regardless of what they actually contain.
            return self._object_type(name, series)
        if pd.api.types.is_string_dtype(dtype):
            return self._string_type(series)
        return self._object_type(name, series)

    def column_types_for(self, obj: pd.DataFrame) -> dict[str, str]:
        """Teradata type of every column, computed once per materialization.

        String and object columns are sized by scanning their values, so this is
        evaluated a single time and reused for the DDL, the row conversion and the
        metadata rather than once for each.
        """
        return {
            str(name): self.column_type(str(name), obj[name]) for name in obj.columns
        }

    @staticmethod
    def _to_python(value: Any) -> Any:
        """Convert a pandas/numpy scalar into something the driver accepts."""
        if value is None:
            return None
        if isinstance(value, pd.Timestamp):
            return value.to_pydatetime()
        if isinstance(value, np.generic):
            value = value.item()
        return bindable_int(value)

    def _rows(
        self, obj: pd.DataFrame, string_columns: frozenset[str]
    ) -> list[list[Any]]:
        # Values destined for a VARCHAR/CLOB column must be converted to strings here:
        # the column can hold arbitrary objects (dicts, UUIDs, tuples, ...) that the
        # driver cannot bind directly, even though the DDL declares a string type.
        columns = [str(name) for name in obj.columns]
        decimal_columns = _uint64_positions(obj)
        frame = obj.astype(object).where(pd.notna(obj), None)
        rows = []
        for row in frame.itertuples(index=False, name=None):
            converted_row = []
            for position, (name, value) in enumerate(zip(columns, row)):
                value = self._to_python(value)
                if value is not None and name in string_columns:
                    value = str(value)
                elif value is not None and position in decimal_columns:
                    value = Decimal(value)
                converted_row.append(value)
            rows.append(converted_row)
        return rows

    def handle_output(
        self,
        context: OutputContext,
        table_slice: TableSlice,
        obj: pd.DataFrame,
        connection: Any,
    ) -> Mapping[str, Any]:
        if not isinstance(obj, pd.DataFrame):
            raise TypeError(
                f"TeradataPandasTypeHandler can only store pandas DataFrames, got "
                f"{type(obj)}."
            )
        if obj.columns.empty:
            raise ValueError(
                "Cannot store a DataFrame with no columns: Teradata tables require at "
                "least one column."
            )
        if obj.columns.duplicated().any():
            duplicates = sorted(
                {str(name) for name in obj.columns[obj.columns.duplicated()]}
            )
            raise ValueError(
                f"Cannot store a DataFrame with duplicate column names: {duplicates}. "
                "Teradata table and INSERT column lists require unique identifiers."
            )
        # Columns are quoted as str(name) in the generated DDL, so labels such as 1
        # and "1" collide too, not just names that only differ by letter case.
        case_insensitive = case_insensitive_duplicates(
            str(name) for name in obj.columns
        )
        if case_insensitive:
            raise ValueError(
                "Cannot store a DataFrame with column names that only differ by type "
                f"or letter case: {case_insensitive}. Teradata identifiers "
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
                        self._rows(
                            obj.iloc[start : start + self.chunk_size], string_columns
                        )
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

    def load_input(
        self, context: InputContext, table_slice: TableSlice, connection: Any
    ) -> pd.DataFrame:
        with connection.cursor() as cursor:
            # get_select_statement applies both column selection and the partition
            # WHERE clause, so partitioned inputs only read their own rows.
            cursor.execute(TeradataDbClient.get_select_statement(table_slice))
            column_names = [description[0] for description in cursor.description]
            rows = cursor.fetchall()
        if not rows:
            return pd.DataFrame(columns=column_names)
        return pd.DataFrame(rows, columns=column_names).infer_objects()

    @property
    def supported_types(self) -> Sequence[type]:
        return [pd.DataFrame]


class TeradataPandasIOManager(TeradataIOManager):
    """An I/O manager that stores pandas DataFrames as Teradata tables.

    Example:
        .. code-block:: python

            from dagster import Definitions, EnvVar, asset
            from dagster_teradata import TeradataPandasIOManager, TeradataResource

            @asset(key_prefix=["analytics"])
            def customers() -> pd.DataFrame:
                return pd.DataFrame({"id": [1, 2]})

            defs = Definitions(
                assets=[customers],
                resources={
                    "io_manager": TeradataPandasIOManager(
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
            TeradataPandasTypeHandler(
                chunk_size=self.chunk_size,
                min_varchar_length=self.min_varchar_length,
                column_types=self.column_types,
            )
        ]

    @staticmethod
    def default_load_type() -> type | None:
        return pd.DataFrame


teradata_pandas_io_manager = build_teradata_io_manager(
    [TeradataPandasTypeHandler()], default_load_type=pd.DataFrame
)
"""Legacy ``@io_manager``-style equivalent of :py:class:`TeradataPandasIOManager`."""

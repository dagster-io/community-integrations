"""Teradata datatype coverage and cross-handler parity tests.

Every ``DbTypeHandler`` shipped with ``dagster_teradata`` derives the Teradata
column type used in ``CREATE TABLE`` from the in-memory DataFrame's schema. This
module pins that mapping for all three handlers in a single table so that:

* every Teradata type the library can emit is exercised (``TYPE_CASES``);
* a mapping cannot silently disappear (``test_every_teradata_type_is_covered``);
* the handlers cannot silently drift apart for a logical type they all support
  (``test_pandas_and_polars_agree``, ``test_pyspark_agrees_where_spark_has_the_type``).

The mapping is asserted through the public ``column_type`` methods rather than by
issuing DDL, so these stay fast unit tests. ``functional/test_io_manager_datatypes.py``
round-trips the same matrix through a live Teradata system.
"""

import datetime
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal

import pandas as pd
import pytest

from dagster_teradata import TeradataResource
from dagster_teradata.pandas_type_handler import TeradataPandasTypeHandler


pytest.importorskip("polars")
pytest.importorskip("pyspark")

import polars as pl  # noqa: E402
from pyspark.sql import types as T  # noqa: E402
from dagster_teradata.polars_type_handler import TeradataPolarsTypeHandler  # noqa: E402
from dagster_teradata.pyspark_type_handler import TeradataPySparkTypeHandler  # noqa: E402

# Spark's schema carries no string length, so the PySpark handler falls back to a
# fixed default width while pandas/polars size VARCHAR from the data they see.
_SPARK_DEFAULT_VARCHAR = "VARCHAR(1024) CHARACTER SET UNICODE"


@dataclass(frozen=True)
class TypeCase:
    """One logical type and the Teradata column type each handler maps it to.

    ``pandas``/``polars`` are factories (rather than Series) so that constructing
    a Series for one handler cannot fail collection for the others. A ``None``
    field means the library deliberately has no mapping for that handler, which
    the parity tests treat as "not comparable" rather than as a failure.
    """

    logical: str
    teradata: str
    pandas: Callable[[], pd.Series] | None = None
    polars: Callable[[], pl.Series] | None = None
    spark: T.DataType | None = None
    # Set when a handler intentionally maps the logical type differently.
    spark_teradata: str | None = None
    # Why a handler has no mapping, for documentation value in the table below.
    note: str = ""

    @property
    def expected_spark(self) -> str:
        return self.spark_teradata or self.teradata


TYPE_CASES: list[TypeCase] = [
    TypeCase(
        logical="boolean",
        teradata="BYTEINT",
        pandas=lambda: pd.Series([True, False]),
        polars=lambda: pl.Series([True, False]),
        spark=T.BooleanType(),
    ),
    TypeCase(
        logical="int8",
        teradata="BYTEINT",
        pandas=lambda: pd.Series([1], dtype="int8"),
        polars=lambda: pl.Series([1], dtype=pl.Int8),
        spark=T.ByteType(),
    ),
    TypeCase(
        logical="int16",
        teradata="SMALLINT",
        pandas=lambda: pd.Series([1], dtype="int16"),
        polars=lambda: pl.Series([1], dtype=pl.Int16),
        spark=T.ShortType(),
    ),
    TypeCase(
        logical="int32",
        teradata="INTEGER",
        pandas=lambda: pd.Series([1], dtype="int32"),
        polars=lambda: pl.Series([1], dtype=pl.Int32),
        spark=T.IntegerType(),
    ),
    TypeCase(
        logical="int64",
        teradata="BIGINT",
        pandas=lambda: pd.Series([1], dtype="int64"),
        polars=lambda: pl.Series([1], dtype=pl.Int64),
        spark=T.LongType(),
    ),
    # Teradata integer types are all signed, so unsigned dtypes widen by one step.
    # Spark has no unsigned integer types at all.
    TypeCase(
        logical="uint8",
        teradata="SMALLINT",
        pandas=lambda: pd.Series([1], dtype="uint8"),
        polars=lambda: pl.Series([1], dtype=pl.UInt8),
        note="Spark has no unsigned integer types.",
    ),
    TypeCase(
        logical="uint16",
        teradata="INTEGER",
        pandas=lambda: pd.Series([1], dtype="uint16"),
        polars=lambda: pl.Series([1], dtype=pl.UInt16),
        note="Spark has no unsigned integer types.",
    ),
    TypeCase(
        logical="uint32",
        teradata="BIGINT",
        pandas=lambda: pd.Series([1], dtype="uint32"),
        polars=lambda: pl.Series([1], dtype=pl.UInt32),
        note="Spark has no unsigned integer types.",
    ),
    TypeCase(
        logical="uint64",
        # BIGINT is too narrow for uint64's full range, so a DECIMAL is required.
        teradata="DECIMAL(20,0)",
        pandas=lambda: pd.Series([1], dtype="uint64"),
        polars=lambda: pl.Series([1], dtype=pl.UInt64),
        note="Spark has no unsigned integer types.",
    ),
    TypeCase(
        logical="float32",
        teradata="FLOAT",
        pandas=lambda: pd.Series([1.5], dtype="float32"),
        polars=lambda: pl.Series([1.5], dtype=pl.Float32),
        spark=T.FloatType(),
    ),
    TypeCase(
        logical="float64",
        teradata="FLOAT",
        pandas=lambda: pd.Series([1.5], dtype="float64"),
        polars=lambda: pl.Series([1.5], dtype=pl.Float64),
        spark=T.DoubleType(),
    ),
    TypeCase(
        logical="decimal",
        teradata="DECIMAL(5,3)",
        # pandas has no decimal dtype; precision/scale are derived from the values.
        pandas=lambda: pd.Series([Decimal("12.345")], dtype=object),
        polars=lambda: pl.Series(["12.345"], dtype=pl.Decimal(5, 3)),
        spark=T.DecimalType(5, 3),
    ),
    TypeCase(
        logical="string",
        # pandas/polars size VARCHAR from the observed data (2x width, floor 256).
        teradata="VARCHAR(256) CHARACTER SET UNICODE",
        pandas=lambda: pd.Series(["ab"]),
        polars=lambda: pl.Series(["ab"]),
        spark=T.StringType(),
        spark_teradata=_SPARK_DEFAULT_VARCHAR,
    ),
    TypeCase(
        logical="string_wide",
        # Wider than Teradata's maximum VARCHAR, so it must become a CLOB.
        teradata="CLOB CHARACTER SET UNICODE",
        pandas=lambda: pd.Series(["x" * 20_000]),
        polars=lambda: pl.Series(["x" * 20_000]),
        note="Spark schemas carry no length, so width cannot be inferred.",
    ),
    TypeCase(
        logical="date",
        teradata="DATE",
        pandas=lambda: pd.Series([datetime.date(2024, 1, 1)], dtype=object),
        polars=lambda: pl.Series([datetime.date(2024, 1, 1)]),
        spark=T.DateType(),
    ),
    TypeCase(
        logical="time",
        teradata="TIME(6)",
        pandas=lambda: pd.Series([datetime.time(1, 2)], dtype=object),
        polars=lambda: pl.Series([datetime.time(1, 2)]),
        note="Spark has no time-of-day type.",
    ),
    TypeCase(
        logical="timestamp",
        teradata="TIMESTAMP(6)",
        pandas=lambda: pd.Series(pd.to_datetime(["2024-01-01 10:00:00"])),
        polars=lambda: pl.Series([datetime.datetime(2024, 1, 1, 10, 0)]),
        spark=T.TimestampNTZType(),
    ),
    TypeCase(
        logical="timestamp_tz",
        teradata="TIMESTAMP(6) WITH TIME ZONE",
        pandas=lambda: pd.Series(
            pd.to_datetime(["2024-01-01 10:00:00"]).tz_localize("UTC")
        ),
        polars=lambda: pl.Series(
            [datetime.datetime(2024, 1, 1, 10, 0)]
        ).dt.replace_time_zone("UTC"),
        note=(
            "Spark's TimestampType is an instant rendered in the session time zone; "
            "the JDBC driver binds it as a local java.sql.Timestamp, so it maps to a "
            "plain TIMESTAMP(6) like every other Spark JDBC integration."
        ),
    ),
    TypeCase(
        logical="binary",
        teradata="BLOB",
        pandas=lambda: pd.Series([b"\x00\x01"], dtype=object),
        polars=lambda: pl.Series([b"\x00\x01"], dtype=pl.Binary),
        spark=T.BinaryType(),
    ),
    TypeCase(
        logical="spark_timestamp_ltz",
        teradata="TIMESTAMP(6)",
        spark=T.TimestampType(),
        note="Spark-only: see the timestamp_tz note above.",
    ),
]

# Every distinct Teradata type the library is able to emit. Parameter lists are
# stripped so that e.g. VARCHAR(256) and VARCHAR(1024) count as one type.
EXPECTED_TERADATA_TYPES = {
    "BIGINT",
    "BLOB",
    "BYTEINT",
    "CLOB",
    "DATE",
    "DECIMAL",
    "FLOAT",
    "INTEGER",
    "SMALLINT",
    "TIME",
    "TIMESTAMP",
    "TIMESTAMP WITH TIME ZONE",
    "VARCHAR",
}


def _base_type(teradata_type: str) -> str:
    """Strip the parameter list: 'DECIMAL(5,3)' -> 'DECIMAL'."""
    # The character set is an attribute of the column, not a distinct type.
    teradata_type = teradata_type.removesuffix(" CHARACTER SET UNICODE")
    head, _, tail = teradata_type.partition("(")
    # Preserve the trailing modifier of e.g. 'TIMESTAMP(6) WITH TIME ZONE'.
    _, _, modifier = tail.partition(")")
    return f"{head.strip()}{modifier.rstrip()}"


@pytest.fixture(scope="module")
def pandas_handler() -> TeradataPandasTypeHandler:
    return TeradataPandasTypeHandler()


@pytest.fixture(scope="module")
def polars_handler() -> TeradataPolarsTypeHandler:
    return TeradataPolarsTypeHandler()


@pytest.fixture(scope="module")
def pyspark_handler() -> TeradataPySparkTypeHandler:
    return TeradataPySparkTypeHandler(
        TeradataResource(host="localhost", user="dbc", password="dbc")
    )


def _cases_with(attribute: str) -> list[TypeCase]:
    return [case for case in TYPE_CASES if getattr(case, attribute) is not None]


def _ids(cases: list[TypeCase]) -> list[str]:
    return [case.logical for case in cases]


# --------------------------------------------------------------------------------------
# Per-handler datatype matrix
# --------------------------------------------------------------------------------------

_PANDAS_CASES = _cases_with("pandas")
_POLARS_CASES = _cases_with("polars")
_SPARK_CASES = _cases_with("spark")


@pytest.mark.parametrize("case", _PANDAS_CASES, ids=_ids(_PANDAS_CASES))
def test_pandas_datatype_matrix(pandas_handler, case: TypeCase):
    assert pandas_handler.column_type("col", case.pandas()) == case.teradata


@pytest.mark.parametrize("case", _POLARS_CASES, ids=_ids(_POLARS_CASES))
def test_polars_datatype_matrix(polars_handler, case: TypeCase):
    assert polars_handler.column_type("col", case.polars()) == case.teradata


@pytest.mark.parametrize("case", _SPARK_CASES, ids=_ids(_SPARK_CASES))
def test_pyspark_datatype_matrix(pyspark_handler, case: TypeCase):
    field = T.StructField("col", case.spark)
    assert pyspark_handler.column_type(field) == case.expected_spark


# --------------------------------------------------------------------------------------
# Cross-handler parity
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "case",
    [c for c in TYPE_CASES if c.pandas and c.polars],
    ids=[c.logical for c in TYPE_CASES if c.pandas and c.polars],
)
def test_pandas_and_polars_agree(pandas_handler, polars_handler, case: TypeCase):
    """pandas and polars have equivalent type systems, so they must never diverge."""
    assert pandas_handler.column_type(
        "col", case.pandas()
    ) == polars_handler.column_type("col", case.polars())


@pytest.mark.parametrize(
    "case",
    [c for c in TYPE_CASES if c.spark and c.pandas and not c.spark_teradata],
    ids=[
        c.logical for c in TYPE_CASES if c.spark and c.pandas and not c.spark_teradata
    ],
)
def test_pyspark_agrees_where_spark_has_the_type(
    pandas_handler, pyspark_handler, case: TypeCase
):
    """Where Spark has an equivalent type, it must map to the same Teradata type.

    Strings are excluded (``spark_teradata`` is set for them) because Spark's
    schema carries no length, so the handler cannot size the VARCHAR from data.
    """
    field = T.StructField("col", case.spark)
    assert pandas_handler.column_type(
        "col", case.pandas()
    ) == pyspark_handler.column_type(field)


def test_only_strings_differ_between_pyspark_and_the_others():
    """Pin the single intentional divergence so a new one cannot slip in unnoticed."""
    diverging = {c.logical for c in TYPE_CASES if c.spark_teradata}
    assert diverging == {"string"}


# --------------------------------------------------------------------------------------
# Coverage
# --------------------------------------------------------------------------------------


def test_every_teradata_type_is_covered():
    """The matrix must exercise every Teradata type the handlers can emit."""
    covered = {_base_type(case.teradata) for case in TYPE_CASES}
    covered |= {_base_type(case.expected_spark) for case in _SPARK_CASES}
    assert covered == EXPECTED_TERADATA_TYPES


@pytest.mark.parametrize("case", TYPE_CASES, ids=_ids(TYPE_CASES))
def test_every_case_reaches_at_least_one_handler(case: TypeCase):
    """Guards against a case that silently tests nothing."""
    assert case.pandas or case.polars or case.spark
    if not (case.pandas and case.polars and case.spark):
        assert case.note, (
            f"'{case.logical}' is not mapped by every handler, so it must document "
            "why in TypeCase.note."
        )


def test_handlers_emit_the_declared_types_in_generated_ddl(pandas_handler):
    """End-to-end check that column_type() is what actually lands in CREATE TABLE."""
    from unittest.mock import MagicMock

    from dagster._core.storage.db_io_manager import TableSlice

    frame = pd.DataFrame(
        {
            # Every column is truncated to one row: pd.DataFrame() aligns Series on
            # their index, and a shorter column would be NaN-padded, silently
            # widening integer dtypes to float and defeating the assertion below.
            case.logical: case.pandas().head(1).reset_index(drop=True)
            for case in _PANDAS_CASES
        }
    )
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = None  # table does not exist yet

    pandas_handler.handle_output(
        MagicMock(),
        TableSlice(table="t", schema="db", database=None),
        frame,
        connection,
    )

    create = next(
        call.args[0]
        for call in cursor.execute.call_args_list
        if call.args[0].startswith("CREATE TABLE")
    )
    for case in _PANDAS_CASES:
        assert f'"{case.logical}" {case.teradata}' in create

"""Live Teradata datatype round-trip tests for all three type handlers.

These tests are skipped unless the standard Teradata environment variables are
set::

    TERADATA_HOST, TERADATA_USER, TERADATA_PASSWORD, TERADATA_DATABASE

Run them with::

    pytest dagster_teradata_tests/functional/test_io_manager_datatypes.py -v

They complement ``dagster_teradata_tests/test_type_handler_datatypes.py``: that
module pins the dtype -> Teradata type mapping as fast unit tests, while these
actually create the table, write every datatype, read it back through the I/O
manager and assert the values survive the round trip. They also verify the
Teradata catalog agrees with the type the handler said it would create.

The PySpark tests additionally require a Spark installation with the Teradata
JDBC driver on the classpath, pointed at by::

    TERADATA_JDBC_JAR=/path/to/terajdbc4.jar

and are skipped when it is absent.
"""

import datetime
import os
from decimal import Decimal

import pandas as pd
import pytest
import teradatasql
from dagster import AssetIn, asset, materialize

from dagster_teradata import TeradataPandasIOManager, TeradataResource

# Imported only after the skip: the package's lazy export of the polars manager
# raises ImportError when polars is absent, which would fail collection.
pytest.importorskip("polars")

import polars as pl  # noqa: E402

from dagster_teradata import TeradataPolarsIOManager  # noqa: E402

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

# One row exercising every datatype the pandas and polars handlers can emit.
# Kept in sync with TYPE_CASES in dagster_teradata_tests/test_type_handler_datatypes.py.
_TIMESTAMP = datetime.datetime(2024, 1, 2, 3, 4, 5)
_TIMESTAMP_TZ = datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)
# Wide enough that its doubled, headroom-padded width passes Teradata's 32000
# VARCHAR limit, so the pandas and polars handlers declare CLOB for it.
_CLOB_VALUE = "clob-" * 4_000

# column name -> (value, expected Teradata type)
DATATYPE_ROW: dict[str, tuple[object, str]] = {
    "c_boolean": (True, "BYTEINT"),
    "c_int8": (7, "BYTEINT"),
    "c_int16": (300, "SMALLINT"),
    "c_int32": (70_000, "INTEGER"),
    "c_int64": (5_000_000_000, "BIGINT"),
    "c_uint64": (18_446_744_073_709_551_615, "DECIMAL(20,0)"),
    "c_float": (1.5, "FLOAT"),
    "c_decimal": (Decimal("12.345"), "DECIMAL(5,3)"),
    "c_string": ("hello", "VARCHAR(256)"),
    "c_date": (datetime.date(2024, 1, 2), "DATE"),
    "c_time": (datetime.time(3, 4, 5), "TIME(6)"),
    "c_timestamp": (_TIMESTAMP, "TIMESTAMP(6)"),
    "c_timestamp_tz": (_TIMESTAMP_TZ, "TIMESTAMP(6) WITH TIME ZONE"),
    "c_binary": (b"\x00\x01\x02", "BLOB"),
    "c_clob": (_CLOB_VALUE, "CLOB"),
}


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


def _drop_table(connection, table: str) -> None:
    database = os.getenv("TERADATA_DATABASE")
    try:
        connection.cursor().execute(f'DROP TABLE "{database}"."{table}"')
    except teradatasql.DatabaseError:
        pass


def _catalog_columns(connection, table: str) -> dict[str, tuple]:
    """Read the created column definitions back out of the Teradata catalog.

    ``ColumnType`` is a short code (``I`` = INTEGER, ``DA`` = DATE, ...); the
    remaining attributes carry the parameters (VARCHAR length, DECIMAL
    precision/scale, temporal fractional-seconds precision) that the code alone
    does not express, so the assertions can compare the *full* type the handler
    declared rather than only its base.
    """
    database = os.getenv("TERADATA_DATABASE")
    cursor = connection.cursor()
    cursor.execute(
        "SELECT ColumnName, ColumnType, ColumnLength, DecimalTotalDigits, "
        "DecimalFractionalDigits, CharType FROM DBC.ColumnsV "
        "WHERE UPPER(DatabaseName) = UPPER(?) AND UPPER(TableName) = UPPER(?)",
        [database, table],
    )
    return {
        row[0].strip().lower(): (
            row[1].strip(),
            row[2],
            row[3],
            row[4],
            row[5],
        )
        for row in cursor.fetchall()
    }


# Teradata's DBC.ColumnsV type codes for the types this matrix produces.
# These were read back off a live system rather than taken from the manual --
# note BYTEINT is "I1" (not "BY", which is fixed-width BYTE) and TIMESTAMP WITH
# TIME ZONE is "SZ" (not "TZ", which is TIME WITH TIME ZONE).
_TERADATA_TYPE_CODES = {
    "BYTEINT": "I1",
    "SMALLINT": "I2",
    "INTEGER": "I",
    "BIGINT": "I8",
    "FLOAT": "F",
    "DATE": "DA",
    "BLOB": "BO",
    "CLOB": "CO",
}


def _expected_code(teradata_type: str) -> str | None:
    base = teradata_type.split("(", 1)[0].strip()
    if base == "DECIMAL":
        return "D"
    if base == "VARCHAR":
        return "CV"
    if teradata_type.upper().endswith("WITH TIME ZONE"):
        return "SZ" if base == "TIMESTAMP" else "TZ"
    if base == "TIMESTAMP":
        return "TS"
    if base == "TIME":
        return "AT"
    return _TERADATA_TYPE_CODES.get(base)


_CODE_TO_BASE = {code: name for name, code in _TERADATA_TYPE_CODES.items()}

# BLOB/CLOB are declared without an explicit size, so Teradata substitutes its own
# maximum and the reported ColumnLength carries no information about the handler.
_UNPARAMETERIZED_LOB_CODES = {"BO", "CO"}


def _normalize(teradata_type: str) -> str:
    """Collapse whitespace so declared and reconstructed types compare literally."""
    return " ".join(teradata_type.upper().split()).replace(", ", ",")


def _actual_type(row: tuple) -> str | None:
    """Rebuild the full Teradata type from the catalog attributes.

    Returns ``None`` for codes this matrix does not produce, so an unexpected type
    is reported by the code assertion rather than silently compared as a string.
    """
    code, length, total_digits, fractional_digits, char_type = row
    if code == "CV":
        # ColumnLength is a byte count; a UNICODE column (CharType 2) stores two
        # bytes per character, so convert back to the declared character length.
        chars = length // 2 if char_type == 2 else length
        return f"VARCHAR({chars})"
    if code == "D":
        return f"DECIMAL({total_digits},{fractional_digits})"
    if code == "TS":
        return f"TIMESTAMP({fractional_digits})"
    if code == "SZ":
        return f"TIMESTAMP({fractional_digits}) WITH TIME ZONE"
    if code == "AT":
        return f"TIME({fractional_digits})"
    if code == "TZ":
        return f"TIME({fractional_digits}) WITH TIME ZONE"
    return _CODE_TO_BASE.get(code)


def _assert_catalog_types(connection, table: str) -> None:
    actual = _catalog_columns(connection, table)
    for column, (_value, teradata_type) in DATATYPE_ROW.items():
        expected_code = _expected_code(teradata_type)
        if expected_code is None:
            continue
        row = actual[column]
        assert row[0] == expected_code, (
            f"{column} was declared {teradata_type} (DBC code {expected_code}) but "
            f"Teradata reports {row[0]}."
        )
        if row[0] in _UNPARAMETERIZED_LOB_CODES:
            continue
        actual_type = _actual_type(row)
        assert actual_type is not None and _normalize(actual_type) == _normalize(
            teradata_type
        ), (
            f"{column} was declared {teradata_type} but Teradata created "
            f"{actual_type} (DBC row {row})."
        )


# --------------------------------------------------------------------------------------
# pandas
# --------------------------------------------------------------------------------------


def _pandas_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "c_boolean": pd.Series([True], dtype="bool"),
            "c_int8": pd.Series([7], dtype="int8"),
            "c_int16": pd.Series([300], dtype="int16"),
            "c_int32": pd.Series([70_000], dtype="int32"),
            "c_int64": pd.Series([5_000_000_000], dtype="int64"),
            "c_uint64": pd.Series([18_446_744_073_709_551_615], dtype="uint64"),
            "c_float": pd.Series([1.5], dtype="float64"),
            "c_decimal": pd.Series([Decimal("12.345")], dtype=object),
            "c_string": pd.Series(["hello"]),
            "c_date": pd.Series([datetime.date(2024, 1, 2)], dtype=object),
            "c_time": pd.Series([datetime.time(3, 4, 5)], dtype=object),
            "c_timestamp": pd.Series([_TIMESTAMP], dtype="datetime64[ns]"),
            "c_timestamp_tz": pd.Series(
                pd.to_datetime([_TIMESTAMP]).tz_localize("UTC")
            ),
            "c_binary": pd.Series([b"\x00\x01\x02"], dtype=object),
            "c_clob": pd.Series([_CLOB_VALUE]),
        }
    )


def test_pandas_all_datatypes_round_trip(connection, teradata_resource):
    """Every datatype the pandas handler emits survives a write and a read back."""
    table = "io_dtypes_pandas"
    _drop_table(connection, table)
    io_manager = TeradataPandasIOManager(teradata=teradata_resource)

    @asset(name=table)
    def dtypes_asset() -> pd.DataFrame:
        return _pandas_frame()

    try:
        result = materialize([dtypes_asset], resources={"io_manager": io_manager})
        assert result.success

        _assert_catalog_types(connection, table)

        # Read the row back with a raw cursor, so what Teradata stored is checked
        # independently of the handler's own load_input.
        cursor = connection.cursor()
        cursor.execute(f'SELECT * FROM "{os.getenv("TERADATA_DATABASE")}"."{table}"')
        columns = [d[0].strip().lower() for d in cursor.description]
        row = dict(zip(columns, cursor.fetchone()))

        assert row["c_boolean"] == 1
        assert row["c_int8"] == 7
        assert row["c_int16"] == 300
        assert row["c_int32"] == 70_000
        assert row["c_int64"] == 5_000_000_000
        assert int(row["c_uint64"]) == 18_446_744_073_709_551_615
        assert row["c_float"] == pytest.approx(1.5)
        assert Decimal(str(row["c_decimal"])) == Decimal("12.345")
        assert row["c_string"] == "hello"
        assert str(row["c_date"]) == "2024-01-02"
        assert str(row["c_time"]).startswith("03:04:05")
        assert str(row["c_timestamp"]).startswith("2024-01-02 03:04:05")
        assert str(row["c_timestamp_tz"]).startswith("2024-01-02 03:04:05")
        assert bytes(row["c_binary"]) == b"\x00\x01\x02"
        assert row["c_clob"] == _CLOB_VALUE
    finally:
        _drop_table(connection, table)


def test_pandas_rematerialize_preserves_datatypes(connection, teradata_resource):
    """The table is created once; a second write must reuse it, not alter it."""
    table = "io_dtypes_pandas_again"
    _drop_table(connection, table)
    io_manager = TeradataPandasIOManager(teradata=teradata_resource)

    @asset(name=table)
    def dtypes_asset() -> pd.DataFrame:
        return _pandas_frame()

    try:
        assert materialize([dtypes_asset], resources={"io_manager": io_manager}).success
        first = _catalog_columns(connection, table)
        assert materialize([dtypes_asset], resources={"io_manager": io_manager}).success
        assert _catalog_columns(connection, table) == first

        cursor = connection.cursor()
        cursor.execute(
            f'SELECT COUNT(*) FROM "{os.getenv("TERADATA_DATABASE")}"."{table}"'
        )
        # The cleanup DELETE must have removed the first write's row.
        assert cursor.fetchone()[0] == 1
    finally:
        _drop_table(connection, table)


# --------------------------------------------------------------------------------------
# polars
# --------------------------------------------------------------------------------------


def _polars_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "c_boolean": pl.Series([True], dtype=pl.Boolean),
            "c_int8": pl.Series([7], dtype=pl.Int8),
            "c_int16": pl.Series([300], dtype=pl.Int16),
            "c_int32": pl.Series([70_000], dtype=pl.Int32),
            "c_int64": pl.Series([5_000_000_000], dtype=pl.Int64),
            "c_uint64": pl.Series([18_446_744_073_709_551_615], dtype=pl.UInt64),
            "c_float": pl.Series([1.5], dtype=pl.Float64),
            "c_decimal": pl.Series(["12.345"], dtype=pl.Decimal(5, 3)),
            "c_string": pl.Series(["hello"], dtype=pl.String),
            "c_date": pl.Series([datetime.date(2024, 1, 2)], dtype=pl.Date),
            "c_time": pl.Series([datetime.time(3, 4, 5)], dtype=pl.Time),
            "c_timestamp": pl.Series([_TIMESTAMP], dtype=pl.Datetime("us")),
            "c_timestamp_tz": pl.Series(
                [_TIMESTAMP], dtype=pl.Datetime("us")
            ).dt.replace_time_zone("UTC"),
            "c_binary": pl.Series([b"\x00\x01\x02"], dtype=pl.Binary),
            "c_clob": pl.Series([_CLOB_VALUE], dtype=pl.String),
        }
    )


def test_polars_all_datatypes_round_trip(connection, teradata_resource):
    """Every datatype the polars handler emits survives a write and a read back."""
    table = "io_dtypes_polars"
    _drop_table(connection, table)
    io_manager = TeradataPolarsIOManager(teradata=teradata_resource)

    @asset(name=table)
    def dtypes_asset() -> pl.DataFrame:
        return _polars_frame()

    # Read back through TeradataPolarsTypeHandler.load_input rather than a raw
    # cursor, so the live cursor.description -> polars conversion is exercised.
    loaded: list[pl.DataFrame] = []

    @asset(name=f"{table}_downstream")
    def downstream_asset(io_dtypes_polars: pl.DataFrame) -> None:
        loaded.append(io_dtypes_polars)

    try:
        assert materialize(
            [dtypes_asset, downstream_asset], resources={"io_manager": io_manager}
        ).success

        # The polars handler must create exactly the same columns as the pandas one.
        _assert_catalog_types(connection, table)

        (frame,) = loaded
        assert frame.height == 1
        schema = frame.schema
        for name in ("c_boolean", "c_int8", "c_int16", "c_int32", "c_int64"):
            assert schema[name] == pl.Int64, name
        assert schema["c_uint64"] == pl.Decimal(38, 0)
        assert schema["c_float"] == pl.Float64
        assert isinstance(schema["c_decimal"], pl.Decimal)
        assert schema["c_decimal"].scale == 3
        assert schema["c_string"] == pl.String
        assert schema["c_date"] == pl.Date
        assert schema["c_time"] == pl.Time
        assert schema["c_timestamp"] == pl.Datetime("us")
        assert schema["c_timestamp_tz"] == pl.Datetime("us", "UTC")
        assert schema["c_binary"] == pl.Binary
        assert schema["c_clob"] == pl.String

        row = frame.row(0, named=True)
        assert row["c_boolean"] == 1
        assert row["c_int8"] == 7
        assert row["c_int16"] == 300
        assert row["c_int32"] == 70_000
        assert row["c_int64"] == 5_000_000_000
        assert row["c_uint64"] == Decimal("18446744073709551615")
        assert row["c_float"] == 1.5
        assert row["c_decimal"] == Decimal("12.345")
        assert row["c_string"] == "hello"
        assert row["c_date"] == datetime.date(2024, 1, 2)
        assert row["c_time"] == datetime.time(3, 4, 5)
        assert row["c_timestamp"] == _TIMESTAMP
        assert row["c_timestamp_tz"] == _TIMESTAMP_TZ
        assert row["c_binary"] == b"\x00\x01\x02"
        assert row["c_clob"] == _CLOB_VALUE
    finally:
        _drop_table(connection, table)


def test_polars_and_pandas_create_identical_tables(connection, teradata_resource):
    """Cross-handler parity, verified against the live Teradata catalog."""
    pandas_table = "io_parity_pandas"
    polars_table = "io_parity_polars"
    _drop_table(connection, pandas_table)
    _drop_table(connection, polars_table)

    @asset(name=pandas_table)
    def pandas_asset() -> pd.DataFrame:
        return _pandas_frame()

    @asset(name=polars_table)
    def polars_asset() -> pl.DataFrame:
        return _polars_frame()

    try:
        assert materialize(
            [pandas_asset],
            resources={
                "io_manager": TeradataPandasIOManager(teradata=teradata_resource)
            },
        ).success
        assert materialize(
            [polars_asset],
            resources={
                "io_manager": TeradataPolarsIOManager(teradata=teradata_resource)
            },
        ).success

        assert _catalog_columns(connection, pandas_table) == _catalog_columns(
            connection, polars_table
        )
    finally:
        _drop_table(connection, pandas_table)
        _drop_table(connection, polars_table)


def test_polars_round_trips_nulls(connection, teradata_resource):
    """NULLs must survive in every nullable column, not just be dropped."""
    table = "io_dtypes_polars_nulls"
    _drop_table(connection, table)
    io_manager = TeradataPolarsIOManager(teradata=teradata_resource)

    @asset(name=table)
    def dtypes_asset() -> pl.DataFrame:
        frame = _polars_frame()
        # Append an all-NULL row with the same schema.
        nulls = pl.DataFrame(
            {name: pl.Series([None], dtype=frame[name].dtype) for name in frame.columns}
        )
        return pl.concat([frame, nulls])

    try:
        assert materialize([dtypes_asset], resources={"io_manager": io_manager}).success

        cursor = connection.cursor()
        cursor.execute(
            f'SELECT COUNT(*) FROM "{os.getenv("TERADATA_DATABASE")}"."{table}" '
            "WHERE c_int64 IS NULL"
        )
        assert cursor.fetchone()[0] == 1
    finally:
        _drop_table(connection, table)


@pytest.mark.parametrize("rows", [0, 2], ids=["empty", "all_null"])
def test_polars_load_keeps_zoned_type_without_values(
    connection, teradata_resource, rows
):
    """cursor.description reports zoned and naive timestamps alike, so a zoned
    column with no values to infer from must be typed from the catalog, or the
    next write would declare it TIMESTAMP(6) and lose the time zone."""
    table = f"io_polars_zoned_{rows}"
    _drop_table(connection, table)
    io_manager = TeradataPolarsIOManager(teradata=teradata_resource)

    @asset(name=table)
    def zoned_asset() -> pl.DataFrame:
        return pl.DataFrame(
            {
                "id": pl.Series(range(rows), dtype=pl.Int64),
                "naive": pl.Series([None] * rows, dtype=pl.Datetime("us")),
                "zoned": pl.Series([None] * rows, dtype=pl.Datetime("us", "UTC")),
            }
        )

    loaded: list[pl.DataFrame] = []

    @asset(name=f"{table}_downstream", ins={"upstream": AssetIn(key=table)})
    def downstream_asset(upstream: pl.DataFrame) -> None:
        loaded.append(upstream)

    try:
        assert materialize(
            [zoned_asset, downstream_asset], resources={"io_manager": io_manager}
        ).success
        assert _actual_type(_catalog_columns(connection, table)["zoned"]) == (
            "TIMESTAMP(6) WITH TIME ZONE"
        )
        (frame,) = loaded
        assert frame.height == rows
        assert frame.schema["naive"] == pl.Datetime("us")
        assert frame.schema["zoned"] == pl.Datetime("us", "UTC")
    finally:
        _drop_table(connection, table)


# --------------------------------------------------------------------------------------

JDBC_JAR = os.getenv("TERADATA_JDBC_JAR")

pyspark_required = pytest.mark.skipif(
    not JDBC_JAR,
    reason="Set TERADATA_JDBC_JAR to the Teradata JDBC driver to run PySpark tests.",
)


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("dagster-teradata-datatypes")
        .master("local[2]")
        .config("spark.jars", JDBC_JAR)
        .config("spark.sql.session.timeZone", "UTC")
        # The JDBC driver converts DATE/TIMESTAMP values relative to the JVM's
        # default time zone (java.util.TimeZone.getDefault()), not
        # spark.sql.session.timeZone above -- see TeradataPySparkTypeHandler's
        # _require_utc_jvm_timezone docstring. This must be set before the driver
        # JVM starts, so it has to be a JVM option on the *first* SparkSession
        # created in this process, not a runtime config.
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .getOrCreate()
    )
    try:
        yield session
    finally:
        session.stop()


def _spark_frame(spark):
    from pyspark.sql import types as T

    schema = T.StructType(
        [
            T.StructField("c_boolean", T.BooleanType()),
            T.StructField("c_int8", T.ByteType()),
            T.StructField("c_int16", T.ShortType()),
            T.StructField("c_int32", T.IntegerType()),
            T.StructField("c_int64", T.LongType()),
            T.StructField("c_float", T.DoubleType()),
            T.StructField("c_decimal", T.DecimalType(5, 3)),
            T.StructField("c_string", T.StringType()),
            T.StructField("c_date", T.DateType()),
            T.StructField("c_timestamp", T.TimestampNTZType()),
            T.StructField("c_binary", T.BinaryType()),
        ]
    )
    row = (
        True,
        7,
        300,
        70_000,
        5_000_000_000,
        1.5,
        Decimal("12.345"),
        "hello",
        datetime.date(2024, 1, 2),
        _TIMESTAMP,
        bytearray(b"\x00\x01\x02"),
    )
    return spark.createDataFrame([row], schema=schema)


@pyspark_required
def test_pyspark_all_datatypes_round_trip(connection, teradata_resource, spark):
    """Every datatype the PySpark handler emits survives a JDBC write and read."""
    from dagster_teradata import TeradataPySparkIOManager
    from dagster_teradata.pyspark_type_handler import DEFAULT_STRING_LENGTH
    from pyspark.sql import DataFrame as SparkDataFrame

    table = "io_dtypes_pyspark"
    _drop_table(connection, table)
    io_manager = TeradataPySparkIOManager(teradata=teradata_resource)

    @asset(name=table)
    def dtypes_asset() -> SparkDataFrame:
        return _spark_frame(spark)

    # Consumes dtypes_asset as an input, exercising load_input()'s generated JDBC
    # subquery and Spark schema conversion -- not just handle_output()'s write --
    # so this test actually covers the round trip its name promises, including
    # the temporal-read path the JVM-timezone guard protects. The parameter name
    # matches the "table" literal above so Dagster resolves it as a dependency on
    # dtypes_asset without an explicit AssetIn.
    loaded_rows: list[dict] = []

    @asset(name=f"{table}_downstream")
    def downstream_asset(io_dtypes_pyspark: SparkDataFrame) -> None:
        loaded_rows.extend(row.asDict() for row in io_dtypes_pyspark.collect())

    try:
        assert materialize(
            [dtypes_asset, downstream_asset], resources={"io_manager": io_manager}
        ).success

        actual = {
            name: _actual_type(row)
            for name, row in _catalog_columns(connection, table).items()
        }
        # Same Teradata types as the pandas/polars handlers for every type Spark has.
        assert actual["c_boolean"] == "BYTEINT"
        assert actual["c_int8"] == "BYTEINT"
        assert actual["c_int16"] == "SMALLINT"
        assert actual["c_int32"] == "INTEGER"
        assert actual["c_int64"] == "BIGINT"
        assert actual["c_float"] == "FLOAT"
        assert actual["c_decimal"] == "DECIMAL(5,3)"
        assert actual["c_date"] == "DATE"
        assert actual["c_timestamp"] == "TIMESTAMP(6)"
        assert actual["c_binary"] == "BLOB"
        # Spark schemas carry no string length, hence the fixed default width.
        assert actual["c_string"] == f"VARCHAR({DEFAULT_STRING_LENGTH})"

        cursor = connection.cursor()
        cursor.execute(f'SELECT * FROM "{os.getenv("TERADATA_DATABASE")}"."{table}"')
        columns = [d[0].strip().lower() for d in cursor.description]
        row = dict(zip(columns, cursor.fetchone()))

        assert row["c_boolean"] == 1
        assert row["c_int64"] == 5_000_000_000
        assert Decimal(str(row["c_decimal"])) == Decimal("12.345")
        assert row["c_string"] == "hello"
        assert str(row["c_date"]) == "2024-01-02"
        assert str(row["c_timestamp"]).startswith("2024-01-02 03:04:05")
        assert bytes(row["c_binary"]) == b"\x00\x01\x02"

        # The values loaded back through load_input() (the JDBC read path) must
        # match what was written, including the temporal columns the JVM-timezone
        # guard exists to protect.
        assert len(loaded_rows) == 1
        loaded = loaded_rows[0]
        assert loaded["c_boolean"] == 1
        assert loaded["c_int64"] == 5_000_000_000
        assert loaded["c_string"] == "hello"
        assert loaded["c_date"] == datetime.date(2024, 1, 2)
        # DataFrame.collect() materializes TimestampType as a naive Python
        # datetime.datetime.fromtimestamp() of the correct UTC instant -- i.e. in
        # the *test process's own* local time zone, regardless of the JVM's
        # default time zone or spark.sql.session.timeZone. This is a PySpark
        # collect() quirk, unrelated to the JVM-timezone guard or the correctness
        # of the JDBC read itself, so the value has to be converted back to UTC
        # before comparing it to the naive-UTC value that was written.
        assert (
            loaded["c_timestamp"].astimezone(datetime.timezone.utc).replace(tzinfo=None)
            == _TIMESTAMP
        )
        assert bytes(loaded["c_binary"]) == b"\x00\x01\x02"
    finally:
        _drop_table(connection, table)


@pyspark_required
def test_pyspark_rematerialize_does_not_deadlock(connection, teradata_resource, spark):
    """Regression test for the cleanup-DELETE write lock deadlock.

    delete_table_slice() runs on the teradatasql connection with autocommit
    disabled and takes a Teradata WRITE lock on the table. The JDBC write runs on
    Spark's own sessions and needs an incompatible WRITE lock, so unless the
    handler commits the DELETE first, the second materialization blocks forever.
    """
    from dagster_teradata import TeradataPySparkIOManager
    from pyspark.sql import DataFrame as SparkDataFrame

    table = "io_dtypes_pyspark_again"
    _drop_table(connection, table)
    io_manager = TeradataPySparkIOManager(teradata=teradata_resource)

    @asset(name=table)
    def dtypes_asset() -> SparkDataFrame:
        return _spark_frame(spark)

    try:
        assert materialize([dtypes_asset], resources={"io_manager": io_manager}).success
        # This is the materialization that would hang before the fix: the table now
        # exists, so delete_table_slice() issues a real DELETE that takes the lock.
        assert materialize([dtypes_asset], resources={"io_manager": io_manager}).success

        cursor = connection.cursor()
        cursor.execute(
            f'SELECT COUNT(*) FROM "{os.getenv("TERADATA_DATABASE")}"."{table}"'
        )
        assert cursor.fetchone()[0] == 1
    finally:
        _drop_table(connection, table)


@pyspark_required
def test_pyspark_overlength_string_keeps_previous_rows(
    connection, teradata_resource, spark
):
    """A value wider than the VARCHAR column must fail before the cleanup DELETE
    is committed; otherwise Teradata rejects it inside the JDBC write and the
    previous rows are already gone."""
    from dagster_teradata import TeradataPySparkIOManager
    from pyspark.sql import DataFrame as SparkDataFrame

    table = "io_pyspark_overlength"
    _drop_table(connection, table)
    io_manager = TeradataPySparkIOManager(teradata=teradata_resource, string_length=5)
    values = ["short"]
    # The rematerialization spells the column differently, so the length check
    # also runs through the case-only rename to the table's spelling.
    column = ["S"]

    @asset(name=table)
    def strings_asset() -> SparkDataFrame:
        return spark.createDataFrame(
            [(value,) for value in values], f"{column[0]} string"
        )

    try:
        assert materialize(
            [strings_asset], resources={"io_manager": io_manager}
        ).success
        values[:] = ["short", "too long"]
        column[0] = "s"
        result = materialize(
            [strings_asset],
            resources={"io_manager": io_manager},
            raise_on_error=False,
        )
        assert not result.success
        failure = result.get_step_failure_events()[0].event_specific_data.error
        assert "'S' has a 8-character value but holds 5" in failure.to_string()

        cursor = connection.cursor()
        cursor.execute(f'SELECT s FROM "{os.getenv("TERADATA_DATABASE")}"."{table}"')
        assert [row[0] for row in cursor.fetchall()] == ["short"]
    finally:
        _drop_table(connection, table)


@pyspark_required
def test_pyspark_overlength_string_under_qualified_override_keeps_previous_rows(
    connection, teradata_resource, spark
):
    """A sized character override spelled with an alias and a CHARACTER SET
    clause must still be length-checked before the cleanup is committed."""
    from dagster_teradata import TeradataPySparkIOManager
    from pyspark.sql import DataFrame as SparkDataFrame

    table = "io_pyspark_overlength_override"
    _drop_table(connection, table)
    io_manager = TeradataPySparkIOManager(
        teradata=teradata_resource,
        column_types={"s": "CHARACTER VARYING(5) CHARACTER SET UNICODE"},
    )
    values = ["short"]

    @asset(name=table)
    def strings_asset() -> SparkDataFrame:
        return spark.createDataFrame([(value,) for value in values], "s string")

    try:
        assert materialize(
            [strings_asset], resources={"io_manager": io_manager}
        ).success
        values[:] = ["short", "too long"]
        result = materialize(
            [strings_asset],
            resources={"io_manager": io_manager},
            raise_on_error=False,
        )
        assert not result.success
        failure = result.get_step_failure_events()[0].event_specific_data.error
        assert "'s' has a 8-character value but holds 5" in failure.to_string()

        cursor = connection.cursor()
        cursor.execute(f'SELECT s FROM "{os.getenv("TERADATA_DATABASE")}"."{table}"')
        assert [row[0] for row in cursor.fetchall()] == ["short"]
    finally:
        _drop_table(connection, table)


@pyspark_required
def test_pyspark_widened_qualified_override_is_drift_not_data_loss(
    connection, teradata_resource, spark
):
    """Widening an override written with a CHARACTER SET clause must be reported
    as drift before the cleanup is committed, not skipped."""
    from dagster_teradata import TeradataPySparkIOManager
    from pyspark.sql import DataFrame as SparkDataFrame

    table = "io_pyspark_widened_override"
    _drop_table(connection, table)
    declared = ["VARCHAR(5) CHARACTER SET UNICODE"]
    values = ["short"]

    @asset(name=table)
    def strings_asset() -> SparkDataFrame:
        return spark.createDataFrame([(value,) for value in values], "s string")

    def io_manager():
        return TeradataPySparkIOManager(
            teradata=teradata_resource, column_types={"s": declared[0]}
        )

    try:
        assert materialize(
            [strings_asset], resources={"io_manager": io_manager()}
        ).success
        declared[0] = "VARCHAR(10) CHARACTER SET UNICODE"
        values[:] = ["eight..."]
        result = materialize(
            [strings_asset],
            resources={"io_manager": io_manager()},
            raise_on_error=False,
        )
        assert not result.success
        failure = result.get_step_failure_events()[0].event_specific_data.error
        assert "would store it as VARCHAR(10)" in failure.to_string()

        cursor = connection.cursor()
        cursor.execute(f'SELECT s FROM "{os.getenv("TERADATA_DATABASE")}"."{table}"')
        assert [row[0] for row in cursor.fetchall()] == ["short"]
    finally:
        _drop_table(connection, table)


@pyspark_required
def test_pyspark_nan_is_written_as_null_and_previous_rows_are_replaced(
    connection, teradata_resource, spark
):
    """Teradata FLOAT cannot hold NaN; the write must store NULL instead of
    failing after the cleanup DELETE had been committed."""
    from dagster_teradata import TeradataPySparkIOManager
    from pyspark.sql import DataFrame as SparkDataFrame

    table = "io_pyspark_nan"
    _drop_table(connection, table)
    io_manager = TeradataPySparkIOManager(teradata=teradata_resource)
    rows = [(1, 1.5, 2.5)]

    @asset(name=table)
    def floats_asset() -> SparkDataFrame:
        return spark.createDataFrame(rows, "id long, f float, d double")

    try:
        assert materialize([floats_asset], resources={"io_manager": io_manager}).success
        rows[:] = [(2, float("nan"), float("nan")), (3, 0.5, 0.25)]
        assert materialize([floats_asset], resources={"io_manager": io_manager}).success

        cursor = connection.cursor()
        cursor.execute(
            f'SELECT id, f, d FROM "{os.getenv("TERADATA_DATABASE")}"."{table}" '
            "ORDER BY id"
        )
        assert [tuple(row) for row in cursor.fetchall()] == [
            (2, None, None),
            (3, 0.5, 0.25),
        ]
    finally:
        _drop_table(connection, table)


@pyspark_required
def test_pyspark_not_null_override_keeps_previous_rows(
    connection, teradata_resource, spark
):
    """A NOT NULL override is refused up front: a NULL in the frame would
    otherwise fail in the JDBC write after the cleanup was committed."""
    from dagster_teradata import TeradataPySparkIOManager
    from pyspark.sql import DataFrame as SparkDataFrame

    table = "io_pyspark_not_null"
    _drop_table(connection, table)
    cursor = connection.cursor()
    cursor.execute(
        f'CREATE TABLE "{os.getenv("TERADATA_DATABASE")}"."{table}" '
        "(s VARCHAR(10) NOT NULL) NO PRIMARY INDEX"
    )
    cursor.execute(
        f'INSERT INTO "{os.getenv("TERADATA_DATABASE")}"."{table}" VALUES (\'keep\')'
    )
    connection.commit()
    io_manager = TeradataPySparkIOManager(
        teradata=teradata_resource, column_types={"s": "VARCHAR(10) NOT NULL"}
    )

    @asset(name=table)
    def strings_asset() -> SparkDataFrame:
        return spark.createDataFrame([(None,)], "s string")

    try:
        result = materialize(
            [strings_asset],
            resources={"io_manager": io_manager},
            raise_on_error=False,
        )
        assert not result.success
        failure = result.get_step_failure_events()[0].event_specific_data.error
        assert "constraint" in failure.to_string()

        cursor.execute(f'SELECT s FROM "{os.getenv("TERADATA_DATABASE")}"."{table}"')
        assert [row[0] for row in cursor.fetchall()] == ["keep"]
    finally:
        _drop_table(connection, table)


@pyspark_required
def test_pyspark_rename_keeps_columns_with_dots_and_backticks(
    connection, teradata_resource, spark
):
    """Teradata allows '.' and '`' in quoted column names. Rematerializing with a
    case-only change in spelling goes through the rename projection, where an
    unquoted obj["a.b"] would be parsed by Spark as a nested field."""
    from dagster_teradata import TeradataPySparkIOManager
    from pyspark.sql import DataFrame as SparkDataFrame
    from pyspark.sql import types as T

    table = "io_pyspark_dotted_names"
    _drop_table(connection, table)
    io_manager = TeradataPySparkIOManager(teradata=teradata_resource)
    columns = ["A.B", "C`D"]
    rows = [(1, "x")]

    @asset(name=table)
    def dotted_asset() -> SparkDataFrame:
        schema = T.StructType(
            [
                T.StructField(columns[0], T.IntegerType()),
                T.StructField(columns[1], T.StringType()),
            ]
        )
        return spark.createDataFrame(rows, schema)

    try:
        assert materialize([dotted_asset], resources={"io_manager": io_manager}).success
        columns[:] = ["a.b", "c`d"]
        rows[:] = [(2, "y")]
        assert materialize([dotted_asset], resources={"io_manager": io_manager}).success

        cursor = connection.cursor()
        cursor.execute(
            f'SELECT "A.B", "C`D" FROM "{os.getenv("TERADATA_DATABASE")}"."{table}"'
        )
        assert [tuple(row) for row in cursor.fetchall()] == [(2, "y")]
    finally:
        _drop_table(connection, table)


@pyspark_required
def test_pyspark_output_lineage_may_read_its_own_table(
    connection, teradata_resource, spark
):
    """Regression test for the cleanup-lock / lineage deadlock.

    The asset's lazy output reads the very table it replaces. count() evaluates
    that JDBC SELECT on Spark's own sessions, so if the handler still held
    delete_table_slice()'s WRITE lock at that point, the SELECT would wait for it
    while the handler waited for count() -- hanging forever.
    """
    from dagster_teradata import TeradataPySparkIOManager, TeradataPySparkTypeHandler
    from pyspark.sql import DataFrame as SparkDataFrame

    table = "io_pyspark_self_read"
    database = os.getenv("TERADATA_DATABASE")
    _drop_table(connection, table)
    io_manager = TeradataPySparkIOManager(teradata=teradata_resource)
    jdbc_options = TeradataPySparkTypeHandler(teradata_resource)._jdbc_options()

    @asset(name=table)
    def growing_asset() -> SparkDataFrame:
        new_row = spark.createDataFrame([(1,)], "id BIGINT")
        cursor = connection.cursor()
        cursor.execute(
            "SELECT 1 FROM DBC.TablesV WHERE UPPER(DatabaseName) = UPPER(?) "
            "AND UPPER(TableName) = UPPER(?)",
            [database, table],
        )
        if cursor.fetchone() is None:
            return new_row
        existing = (
            spark.read.format("jdbc")
            .options(dbtable=f'"{database}"."{table}"', **jdbc_options)
            .load()
            .selectExpr("CAST(id AS BIGINT) AS id")
        )
        return existing.unionByName(new_row)

    try:
        for expected in (1, 2, 3):
            assert materialize(
                [growing_asset], resources={"io_manager": io_manager}
            ).success
            cursor = connection.cursor()
            cursor.execute(f'SELECT COUNT(*) FROM "{database}"."{table}"')
            assert cursor.fetchone()[0] == expected
    finally:
        _drop_table(connection, table)

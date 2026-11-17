"""Unit tests for the PySpark type handler, using a mocked Teradata connection.

The tests never start a JVM: schema mapping is exercised through plain
``StructType``/``StructField`` objects and DataFrames/SparkSessions are mocked.
"""

from unittest.mock import MagicMock, patch

import pytest
import teradatasql
from dagster._core.storage.db_io_manager import TableSlice
from pydantic import ValidationError

import dagster_teradata
from dagster_teradata import TeradataResource


pytest.importorskip("pyspark")

from pyspark import StorageLevel  # noqa: E402
from pyspark.sql import types as T  # noqa: E402
from dagster_teradata._catalog import (  # noqa: E402
    canonical_teradata_type as _canonical_teradata_type,
)
from dagster_teradata.pyspark_type_handler import (  # noqa: E402
    DEFAULT_BATCH_SIZE,
    DEFAULT_STRING_LENGTH,
    TeradataPySparkIOManager,
    TeradataPySparkTypeHandler,
    _configured_executor_timezone,
)


def make_resource() -> TeradataResource:
    return TeradataResource(
        host="td.example.com", user="dbc", password="dbc", database="analytics"
    )


@pytest.fixture
def handler() -> TeradataPySparkTypeHandler:
    return TeradataPySparkTypeHandler(make_resource())


@pytest.fixture(autouse=True)
def string_lengths():
    """Stub the Spark aggregation over string columns (it needs a live JVM).

    Tests replace ``string_lengths.side_effect`` to simulate the longest values;
    by default every column is empty, so no length check fails.
    """
    with (
        patch.object(
            TeradataPySparkTypeHandler,
            "_max_string_lengths",
            side_effect=lambda frame, names: {name: None for name in names},
        ) as mock,
        patch.object(
            TeradataPySparkTypeHandler,
            "_latin_unrepresentable_counts",
            side_effect=lambda frame, names: {name: 0 for name in names},
        ),
    ):
        yield mock


@pytest.fixture
def existing_table_schema_matches():
    """For tests of an existing table that are not about schema drift: the
    mocked cursor has no catalog rows, which the drift check now rejects."""
    with patch(
        "dagster_teradata.pyspark_type_handler.validate_schema_matches_table",
        return_value={},
    ) as mock:
        yield mock


def make_table_slice(**kwargs) -> TableSlice:
    defaults = {"table": "my_table", "schema": "my_db", "database": None}
    defaults.update(kwargs)
    return TableSlice(**defaults)


def make_frame(columns, schema_fields=None) -> MagicMock:
    """A mock pyspark DataFrame with realistic columns/schema attributes."""
    from pyspark.sql import DataFrame as SparkDataFrame

    frame = MagicMock(spec=SparkDataFrame)
    frame.columns = list(columns)
    frame.schema = T.StructType(
        schema_fields or [T.StructField(name, T.LongType()) for name in columns]
    )
    frame.count.return_value = 2
    # Spark's cache() returns the same DataFrame, so the handler goes on using this
    # mock; repartition() returns a new one, which must behave the same way.
    frame.is_cached = False
    frame.storageLevel = StorageLevel.NONE
    frame.cache.return_value = frame
    # The case-matching rename returns a new, equally usable DataFrame.
    frame.select.return_value = frame
    repartitioned = MagicMock(spec=SparkDataFrame)
    repartitioned.columns = list(columns)
    repartitioned.schema = frame.schema
    repartitioned.count.return_value = 2
    repartitioned.is_cached = False
    repartitioned.storageLevel = StorageLevel.NONE
    repartitioned.cache.return_value = repartitioned
    frame.repartition.return_value = repartitioned
    return frame


def _cursor(connection: MagicMock) -> MagicMock:
    return connection.cursor.return_value.__enter__.return_value


def _executed(cursor: MagicMock) -> list:
    return [call.args[0] for call in cursor.execute.call_args_list]


# --------------------------------------------------------------------------------------
# JDBC URL and options
# --------------------------------------------------------------------------------------


def test_jdbc_url_includes_host_database_and_ansi_tmode(handler):
    assert handler.jdbc_url() == (
        "jdbc:teradata://td.example.com/TMODE=ANSI,CHARSET=UTF8,DATABASE=analytics"
    )


def test_jdbc_url_quotes_a_database_name_with_spaces_or_quotes():
    resource = make_resource().model_copy(update={"database": "sales o'data"})
    url = TeradataPySparkTypeHandler(resource).jdbc_url()
    assert url.endswith("DATABASE='sales o''data'")


def test_jdbc_url_includes_port_and_logmech():
    resource = TeradataResource(
        host="td.example.com", user="dbc", password="dbc", port="1025", logmech="LDAP"
    )
    handler = TeradataPySparkTypeHandler(resource)
    url = handler.jdbc_url()
    assert "DBS_PORT=1025" in url
    assert "LOGMECH=LDAP" in url
    assert "DATABASE" not in url


def test_jdbc_url_requires_host():
    handler = TeradataPySparkTypeHandler(TeradataResource(user="dbc", password="dbc"))
    with pytest.raises(ValueError, match="requires the TeradataResource 'host'"):
        handler.jdbc_url()


def test_jdbc_url_forwards_tls_and_https_proxy_settings():
    resource = TeradataResource(
        host="td.example.com",
        sslmode="VERIFY-FULL",
        sslca="/etc/certs/ca.pem",
        sslprotocol="TLSv1.2",
        slcrl=False,
        sslocsp=True,
        https_proxy="http://proxy:8080",
        https_proxy_password="p,w'd",
    )
    url = TeradataPySparkTypeHandler(resource).jdbc_url()
    assert "SSLMODE=VERIFY-FULL" in url
    assert "SSLCA='/etc/certs/ca.pem'" in url
    assert "SSLPROTOCOL=TLSv1.2" in url
    assert "SSLCRL=OFF" in url
    assert "SSLOCSP=ON" in url
    assert "HTTPS_PROXY='http://proxy:8080'" in url
    # Separators are quoted and embedded quotes doubled, so nothing is injected.
    assert "HTTPS_PROXY_PASSWORD='p,w''d'" in url


def test_jdbc_url_forwards_browser_settings_only_for_browser_logmech():
    browser_settings = {"browser": "chrome", "browser_timeout": 30}
    browser = TeradataPySparkTypeHandler(
        TeradataResource(host="td.example.com", logmech="BROWSER", **browser_settings)
    ).jdbc_url()
    assert "BROWSER=chrome" in browser
    assert "BROWSER_TIMEOUT=30" in browser
    other = TeradataPySparkTypeHandler(
        TeradataResource(host="td.example.com", **browser_settings)
    ).jdbc_url()
    assert "BROWSER" not in other


def test_jdbc_url_warns_about_http_proxy():
    resource = TeradataResource(host="td.example.com", http_proxy="http://p:80")
    with patch("dagster_teradata.pyspark_type_handler.get_dagster_logger") as logger:
        url = TeradataPySparkTypeHandler(resource).jdbc_url()
    assert "PROXY" not in url
    assert "http_proxy" in logger.return_value.warning.call_args.args[0]


def test_jdbc_options(handler):
    options = handler._jdbc_options()
    assert options["url"] == handler.jdbc_url()
    assert options["driver"] == "com.teradata.jdbc.TeraDriver"
    assert options["user"] == "dbc"
    assert options["password"] == "dbc"


# --------------------------------------------------------------------------------------
# schema mapping
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("data_type", "expected"),
    [
        (T.ByteType(), "BYTEINT"),
        (T.ShortType(), "SMALLINT"),
        (T.IntegerType(), "INTEGER"),
        (T.LongType(), "BIGINT"),
        (T.FloatType(), "FLOAT"),
        (T.DoubleType(), "FLOAT"),
        (T.BooleanType(), "BYTEINT"),
        (T.DateType(), "DATE"),
        (T.TimestampType(), "TIMESTAMP(6)"),
        (T.TimestampNTZType(), "TIMESTAMP(6)"),
        (T.BinaryType(), "BLOB"),
    ],
)
def test_column_type_mapping(handler, data_type, expected):
    assert handler.column_type(T.StructField("col", data_type)) == expected


def test_column_type_rejects_void_columns(handler):
    # Spark has no JDBC type for void, so a VARCHAR column created for it could
    # never actually be written -- and the write would fail only after the DDL and
    # the cleanup DELETE had been committed. Reject it while it is still harmless.
    with pytest.raises(ValueError, match="type void"):
        handler.column_type(T.StructField("col", T.NullType()))


def test_string_columns_use_configured_length(handler):
    assert handler.column_type(T.StructField("s", T.StringType())) == (
        "VARCHAR(1024) CHARACTER SET UNICODE"
    )
    custom = TeradataPySparkTypeHandler(make_resource(), string_length=512)
    assert custom.column_type(T.StructField("s", T.StringType())) == (
        "VARCHAR(512) CHARACTER SET UNICODE"
    )


def test_decimal_type_keeps_precision_and_scale(handler):
    field = T.StructField("amount", T.DecimalType(18, 4))
    assert handler.column_type(field) == "DECIMAL(18,4)"


def test_decimal_type_rejects_precision_beyond_max(handler):
    field = T.StructField("amount", T.DecimalType(39, 0))
    with pytest.raises(
        ValueError, match="exceeds Teradata's maximum DECIMAL precision"
    ):
        handler.column_type(field)


@pytest.mark.parametrize(
    "data_type",
    [
        T.ArrayType(T.IntegerType()),
        T.MapType(T.StringType(), T.IntegerType()),
        T.StructType([T.StructField("x", T.IntegerType())]),
    ],
)
def test_nested_types_raise_actionable_error(handler, data_type):
    with pytest.raises(ValueError, match="unsupported nested type"):
        handler.column_type(T.StructField("nested", data_type))


def test_explicit_column_types_override_inference():
    handler = TeradataPySparkTypeHandler(
        make_resource(), column_types={"id": "DECIMAL(38,0)"}
    )
    assert handler.column_type(T.StructField("id", T.LongType())) == "DECIMAL(38,0)"


def test_invalid_constructor_arguments():
    with pytest.raises(ValueError, match="batch_size"):
        TeradataPySparkTypeHandler(make_resource(), batch_size=0)
    with pytest.raises(ValueError, match="string_length"):
        TeradataPySparkTypeHandler(make_resource(), string_length=0)
    with pytest.raises(ValueError, match="write_num_partitions"):
        TeradataPySparkTypeHandler(make_resource(), write_num_partitions=0)


# --------------------------------------------------------------------------------------
# handle_output
# --------------------------------------------------------------------------------------


def test_handle_output_requires_utc_jvm_timezone_for_temporal_columns(handler):
    """A non-UTC JVM default time zone silently shifts DATE/TIMESTAMP values written
    over JDBC (java.sql.Date/Timestamp carry no zone of their own), so this must be
    caught before the write rather than corrupting data silently."""
    connection = MagicMock()
    cursor = _cursor(connection)
    cursor.fetchone.return_value = None
    frame = make_frame(
        ["a", "ts"],
        [T.StructField("a", T.LongType()), T.StructField("ts", T.TimestampType())],
    )
    default_timezone = (
        frame.sparkSession._jvm.java.util.TimeZone.getDefault.return_value
    )
    default_timezone.getID.return_value = "Asia/Kolkata"
    default_timezone.hasSameRules.return_value = False

    with pytest.raises(ValueError, match="JVM's default time zone"):
        handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    # Caught before the cleanup DELETE's commit, so the failure is not silent data
    # loss: nothing has been written and the transaction can still roll back cleanly.
    connection.commit.assert_not_called()


def test_handle_output_succeeds_with_utc_jvm_timezone_and_temporal_columns(handler):
    connection = MagicMock()
    cursor = _cursor(connection)
    cursor.fetchone.return_value = None
    frame = make_frame(
        ["a", "ts"],
        [T.StructField("a", T.LongType()), T.StructField("ts", T.TimestampType())],
    )
    frame.sparkSession._jvm.java.util.TimeZone.getDefault.return_value.hasSameRules.return_value = True

    handler.handle_output(MagicMock(), make_table_slice(), frame, connection)


def test_handle_output_skips_jvm_timezone_check_without_temporal_columns(handler):
    """No DATE/TIMESTAMP/TIMESTAMP-NTZ column means nothing to shift, so the check
    must not run at all -- and in particular must not require the mocked
    sparkSession._jvm to be configured, which is what every other handle_output
    test relies on."""
    connection = MagicMock()
    cursor = _cursor(connection)
    cursor.fetchone.return_value = None
    frame = make_frame(
        ["a", "b"],
        [T.StructField("a", T.LongType()), T.StructField("b", T.StringType())],
    )

    # sparkSession is left completely unconfigured: the guard's temporal-column
    # check must short-circuit before ever touching ._jvm on it.
    handler.handle_output(MagicMock(), make_table_slice(), frame, connection)


def test_handle_output_creates_table_and_writes_via_jdbc(handler):
    connection = MagicMock()
    cursor = _cursor(connection)
    cursor.fetchone.return_value = None  # table_exists check: no matching row
    frame = make_frame(
        ["a", "b"],
        [T.StructField("a", T.LongType()), T.StructField("b", T.StringType())],
    )

    metadata = handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    create = next(stmt for stmt in _executed(cursor) if stmt.startswith("CREATE TABLE"))
    assert create == (
        'CREATE TABLE "my_db"."my_table" ("a" BIGINT, '
        '"b" VARCHAR(1024) CHARACTER SET UNICODE) NO PRIMARY INDEX'
    )
    # One commit for the DDL (Teradata requires COMMIT WORK or a null statement to
    # follow DDL) and one releasing the cleanup DELETE's write lock before the
    # JDBC write.
    assert connection.commit.call_count == 2

    writer = frame.write.format.return_value.mode.return_value.options.return_value
    options = frame.write.format.return_value.mode.return_value.options
    frame.write.format.assert_called_once_with("jdbc")
    frame.write.format.return_value.mode.assert_called_once_with("append")
    kwargs = options.call_args.kwargs
    assert kwargs["dbtable"] == '"my_db"."my_table"'
    assert kwargs["batchsize"] == str(DEFAULT_BATCH_SIZE)
    assert kwargs["driver"] == "com.teradata.jdbc.TeraDriver"
    assert kwargs["user"] == "dbc"
    writer.save.assert_called_once_with()

    assert metadata["row_count"] == 2
    schema = metadata["dagster/column_schema"]
    assert [(c.name, c.type) for c in schema.schema.columns] == [
        ("a", "BIGINT"),
        ("b", "VARCHAR(1024) CHARACTER SET UNICODE"),
    ]


@pytest.mark.usefixtures("existing_table_schema_matches")
def test_handle_output_commits_cleanup_delete_before_jdbc_write(handler):
    """The cleanup DELETE runs on `connection` with autocommit disabled and holds a
    Teradata WRITE lock on the target table. The JDBC write uses Spark's own
    sessions, whose INSERTs need an incompatible WRITE lock, so leaving the DELETE
    open would block the write until handle_output returns - a deadlock. The DELETE
    must therefore be committed before the write is issued."""
    connection = MagicMock()
    cursor = _cursor(connection)
    cursor.fetchone.return_value = (1,)  # table already exists: no DDL, no DDL commit
    frame = make_frame(["a"])

    call_order = []
    connection.commit.side_effect = lambda: call_order.append("commit")
    frame.write.format.side_effect = lambda fmt: (
        call_order.append("write") or MagicMock()
    )

    handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    assert call_order == ["commit", "write"]


@pytest.mark.usefixtures("existing_table_schema_matches")
def test_handle_output_releases_cleanup_lock_while_materializing(handler):
    """The frame's lineage may read the target table itself, and count() runs it on
    Spark's own sessions: holding delete_table_slice()'s WRITE lock across it would
    hang. The DELETE is rolled back before count() and re-issued after it."""
    connection = MagicMock()
    cursor = _cursor(connection)
    cursor.fetchone.return_value = (1,)
    frame = make_frame(["a"])

    call_order = []
    connection.rollback.side_effect = lambda: call_order.append("rollback")
    connection.commit.side_effect = lambda: call_order.append("commit")
    frame.count.side_effect = lambda: call_order.append("count") or 2
    cursor.execute.side_effect = lambda statement, *a, **k: call_order.append(
        "delete" if statement.startswith("DELETE") else "sql"
    )
    frame.write.format.side_effect = lambda fmt: (
        call_order.append("write") or MagicMock()
    )

    handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    tail = [step for step in call_order if step != "sql"]
    assert tail == ["rollback", "count", "delete", "commit", "write"]


@pytest.mark.usefixtures("existing_table_schema_matches")
def test_handle_output_lineage_failure_deletes_nothing(handler):
    connection = MagicMock()
    cursor = _cursor(connection)
    cursor.fetchone.return_value = (1,)
    frame = make_frame(["a"])
    frame.count.side_effect = RuntimeError("upstream failed")

    with pytest.raises(RuntimeError, match="upstream failed"):
        handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    connection.rollback.assert_called_once()
    assert not any(s.startswith("DELETE") for s in _executed(cursor))
    connection.commit.assert_not_called()


@pytest.mark.usefixtures("existing_table_schema_matches")
def test_handle_output_skips_create_table_when_already_exists(handler):
    """When the table already exists, CREATE TABLE must not be attempted at all -
    only DML runs, so the DELETE done by delete_table_slice() and the JDBC write can
    proceed without a DDL statement interrupting the transaction."""
    connection = MagicMock()
    cursor = _cursor(connection)
    cursor.fetchone.return_value = (1,)  # table_exists check finds a matching row

    handler.handle_output(
        MagicMock(), make_table_slice(), make_frame(["a"]), connection
    )

    assert not any(stmt.startswith("CREATE TABLE") for stmt in _executed(cursor))
    # No DDL commit, only the commit that releases the cleanup DELETE's lock.
    assert connection.commit.call_count == 1


@pytest.mark.usefixtures("existing_table_schema_matches")
def test_handle_output_ignores_table_already_exists(handler):
    """A failed 'already exists' CREATE TABLE silently discards
    delete_table_slice()'s earlier DELETE, so the DELETE is re-issued before the
    JDBC write, and no commit happens (the failed DDL does not force the post-DDL
    commit-only restriction)."""
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
        MagicMock(), make_table_slice(), make_frame(["a"]), connection
    )

    executed = _executed(cursor)
    # Re-issued once by the failed CREATE path and once more after the frame is
    # materialized (the first is rolled back to free the lock for count()).
    assert executed.count('DELETE FROM "my_db"."my_table"') == 2
    connection.rollback.assert_called_once()
    # No DDL commit (the CREATE TABLE failed), only the commit that releases the
    # re-issued DELETE's lock before the JDBC write.
    assert connection.commit.call_count == 1


def test_handle_output_propagates_other_create_errors(handler):
    connection = MagicMock()
    _cursor(connection).execute.side_effect = teradatasql.DatabaseError(
        "[Error 3523] insufficient privilege"
    )

    with pytest.raises(teradatasql.DatabaseError):
        handler.handle_output(
            MagicMock(), make_table_slice(), make_frame(["a"]), connection
        )


@pytest.mark.usefixtures("existing_table_schema_matches")
def test_handle_output_repartitions_when_configured():
    handler = TeradataPySparkTypeHandler(make_resource(), write_num_partitions=4)
    frame = make_frame(["a"])

    handler.handle_output(MagicMock(), make_table_slice(), frame, MagicMock())

    frame.repartition.assert_called_once_with(4)
    frame.repartition.return_value.write.format.assert_called_once_with("jdbc")


def test_handle_output_rejects_frame_without_columns(handler):
    with pytest.raises(ValueError, match="no columns"):
        handler.handle_output(
            MagicMock(), make_table_slice(), make_frame([]), MagicMock()
        )


def test_handle_output_rejects_case_insensitive_duplicate_column_names(handler):
    # Teradata identifiers are case-insensitive, so "A" and "a" collide even though
    # Spark treats them as distinct column labels.
    frame = make_frame(["A", "a"])
    with pytest.raises(ValueError, match="duplicate column names"):
        handler.handle_output(MagicMock(), make_table_slice(), frame, MagicMock())


def test_handle_output_rejects_exactly_duplicated_column_names(handler):
    # Spark permits exact duplicates too, and the error must describe them
    # accurately rather than claiming they differ only by letter case.
    frame = make_frame(["a", "a"])
    with pytest.raises(ValueError, match="duplicate column names"):
        handler.handle_output(MagicMock(), make_table_slice(), frame, MagicMock())


def test_handle_output_rejects_non_dataframe(handler):
    with pytest.raises(TypeError, match="only store pyspark.sql.DataFrame"):
        handler.handle_output(MagicMock(), make_table_slice(), [1, 2], MagicMock())


# --------------------------------------------------------------------------------------
# load_input
# --------------------------------------------------------------------------------------


def _spark_with_reader() -> MagicMock:
    spark = MagicMock()
    return spark, spark.read.format.return_value.options.return_value


def test_load_input_requires_utc_jvm_timezone_for_temporal_columns(handler):
    """Reading DATE/TIMESTAMP columns over JDBC is subject to the same
    JVM-default-time-zone conversion as writing them, so a load must be rejected
    just as a write would be rather than silently returning shifted values."""
    spark, reader = _spark_with_reader()
    reader.load.return_value.schema = T.StructType(
        [T.StructField("ts", T.TimestampType())]
    )
    default_timezone = spark._jvm.java.util.TimeZone.getDefault.return_value
    default_timezone.getID.return_value = "Asia/Kolkata"
    default_timezone.hasSameRules.return_value = False

    with (
        patch(
            "dagster_teradata.pyspark_type_handler.SparkSession.getActiveSession",
            return_value=spark,
        ),
        pytest.raises(ValueError, match="JVM's default time zone"),
    ):
        handler.load_input(MagicMock(), make_table_slice(columns=["ts"]), MagicMock())


def test_load_input_succeeds_with_utc_jvm_timezone_and_temporal_columns(handler):
    spark, reader = _spark_with_reader()
    reader.load.return_value.schema = T.StructType(
        [T.StructField("ts", T.TimestampType())]
    )
    spark._jvm.java.util.TimeZone.getDefault.return_value.hasSameRules.return_value = (
        True
    )

    with patch(
        "dagster_teradata.pyspark_type_handler.SparkSession.getActiveSession",
        return_value=spark,
    ):
        result = handler.load_input(
            MagicMock(), make_table_slice(columns=["ts"]), MagicMock()
        )

    assert result is reader.load.return_value


def test_load_input_wraps_select_statement_as_subquery(handler):
    spark, reader = _spark_with_reader()

    with patch(
        "dagster_teradata.pyspark_type_handler.SparkSession.getActiveSession",
        return_value=spark,
    ):
        result = handler.load_input(
            MagicMock(), make_table_slice(columns=["a", "b"]), MagicMock()
        )

    options = spark.read.format.return_value.options
    spark.read.format.assert_called_once_with("jdbc")
    kwargs = options.call_args.kwargs
    assert kwargs["dbtable"] == (
        '(SELECT "a", "b" FROM "my_db"."my_table") AS dagster_input'
    )
    assert kwargs["driver"] == "com.teradata.jdbc.TeraDriver"
    reader.load.assert_called_once_with()
    assert result is reader.load.return_value


def test_load_input_forwards_partitioned_read_options():
    handler = TeradataPySparkTypeHandler(
        make_resource(),
        read_partitioning={
            "partitionColumn": "id",
            "lowerBound": 0,
            "upperBound": 1_000_000,
            "numPartitions": 8,
        },
    )
    spark, _ = _spark_with_reader()

    with patch(
        "dagster_teradata.pyspark_type_handler.SparkSession.getActiveSession",
        return_value=spark,
    ):
        handler.load_input(MagicMock(), make_table_slice(), MagicMock())

    kwargs = spark.read.format.return_value.options.call_args.kwargs
    assert kwargs["partitionColumn"] == "id"
    assert kwargs["lowerBound"] == 0
    assert kwargs["upperBound"] == 1_000_000
    assert kwargs["numPartitions"] == 8


def test_load_input_rejects_partition_column_outside_loaded_columns():
    handler = TeradataPySparkTypeHandler(
        make_resource(),
        read_partitioning={
            "partitionColumn": "id",
            "lowerBound": 0,
            "upperBound": 10,
            "numPartitions": 2,
        },
    )
    spark, _ = _spark_with_reader()

    with (
        patch(
            "dagster_teradata.pyspark_type_handler.SparkSession.getActiveSession",
            return_value=spark,
        ),
        pytest.raises(ValueError, match="not among the loaded columns"),
    ):
        handler.load_input(
            MagicMock(), make_table_slice(columns=["a", "b"]), MagicMock()
        )


def test_load_input_requires_active_spark_session(handler):
    with (
        patch(
            "dagster_teradata.pyspark_type_handler.SparkSession.getActiveSession",
            return_value=None,
        ),
        patch(
            "dagster_teradata.pyspark_type_handler.SparkSession._instantiatedSession",
            None,
        ),
        pytest.raises(ValueError, match="requires an active SparkSession"),
    ):
        handler.load_input(MagicMock(), make_table_slice(), MagicMock())


def test_load_input_falls_back_to_default_session_across_threads(handler):
    """getActiveSession() is thread-local and Dagster runs ops on worker threads,
    so a session built on another thread must still be found via the process-wide
    default session rather than failing the load."""
    spark, reader = _spark_with_reader()

    with (
        patch(
            "dagster_teradata.pyspark_type_handler.SparkSession.getActiveSession",
            return_value=None,
        ),
        patch(
            "dagster_teradata.pyspark_type_handler.SparkSession._instantiatedSession",
            spark,
        ),
    ):
        result = handler.load_input(MagicMock(), make_table_slice(), MagicMock())

    assert result is reader.load.return_value


def test_read_partitioning_cannot_override_connection_options():
    handler = TeradataPySparkTypeHandler(
        make_resource(), read_partitioning={"url": "jdbc:teradata://evil/"}
    )
    spark, _ = _spark_with_reader()

    with (
        patch(
            "dagster_teradata.pyspark_type_handler.SparkSession.getActiveSession",
            return_value=spark,
        ),
        pytest.raises(ValueError, match="must not override the reserved options"),
    ):
        handler.load_input(MagicMock(), make_table_slice(), MagicMock())


def test_load_input_rejects_read_partitioning_that_overrides_dbtable():
    # dbtable is passed by load_input() itself rather than coming from
    # _jdbc_options(), so without an explicit reservation it would slip past
    # validation and collide as a duplicate keyword argument.
    handler = TeradataPySparkTypeHandler(
        make_resource(), read_partitioning={"dbtable": "other_table"}
    )
    spark, _ = _spark_with_reader()

    with (
        patch(
            "dagster_teradata.pyspark_type_handler.SparkSession.getActiveSession",
            return_value=spark,
        ),
        pytest.raises(ValueError, match="must not override the reserved options"),
    ):
        handler.load_input(MagicMock(), make_table_slice(), MagicMock())


def test_read_partitioning_collision_error_names_every_reserved_key():
    handler = TeradataPySparkTypeHandler(
        make_resource(), read_partitioning={"dbtable": "t", "url": "jdbc:other"}
    )

    with pytest.raises(ValueError) as excinfo:
        handler._read_options()

    assert "dbtable" in str(excinfo.value)
    assert "url" in str(excinfo.value)


def test_read_partitioning_cannot_supply_credentials_when_resource_has_none():
    # user/password must be reserved even when the resource itself doesn't set
    # them (e.g. LOGMECH-based auth with no explicit user/password): otherwise
    # they'd be absent from _jdbc_options() and read_partitioning could smuggle
    # its own credentials in, undermining the "connection settings only come
    # from TeradataResource" contract.
    resource = TeradataResource(host="td.example.com")
    handler = TeradataPySparkTypeHandler(
        resource, read_partitioning={"user": "evil", "password": "evil"}
    )

    with pytest.raises(ValueError, match="must not override the reserved options"):
        handler._read_options()


def test_read_partitioning_cannot_supply_query():
    # load_input always supplies "dbtable", and Spark rejects a JDBC read that
    # specifies both "dbtable" and "query". Without reserving "query" here, that
    # conflict would surface later as an opaque Spark error instead of this
    # guard's named one.
    handler = TeradataPySparkTypeHandler(
        make_resource(), read_partitioning={"query": "SELECT 1"}
    )

    with pytest.raises(ValueError, match="must not override the reserved options"):
        handler._read_options()


def test_read_partitioning_collision_with_query_is_detected_case_insensitively():
    handler = TeradataPySparkTypeHandler(
        make_resource(), read_partitioning={"QUERY": "SELECT 1"}
    )

    with pytest.raises(ValueError, match="must not override the reserved options"):
        handler._read_options()


def test_supported_types(handler):
    from pyspark.sql import DataFrame as SparkDataFrame

    from dagster_teradata import pyspark_type_handler

    pyspark_type_handler._spark_connect_dataframe_type.cache_clear()
    try:
        with patch.object(
            pyspark_type_handler.importlib,
            "import_module",
            side_effect=ImportError("grpc is not installed"),
        ):
            assert handler.supported_types == [SparkDataFrame]
    finally:
        pyspark_type_handler._spark_connect_dataframe_type.cache_clear()


# --------------------------------------------------------------------------------------
# I/O manager wiring
# --------------------------------------------------------------------------------------


def test_io_manager_passes_configuration_to_handler():
    io_manager = TeradataPySparkIOManager(
        teradata=make_resource(),
        batch_size=500,
        string_length=512,
        column_types={"amount": "DECIMAL(18,4)"},
        read_partitioning={
            "partitionColumn": "id",
            "lowerBound": 0,
            "upperBound": 10,
            "numPartitions": 2,
        },
        write_num_partitions=4,
    )
    (handler,) = io_manager.type_handlers()
    assert handler.batch_size == 500
    assert handler.string_length == 512
    assert handler.column_types == {"amount": "DECIMAL(18,4)"}
    assert handler.read_partitioning["numPartitions"] == 2
    assert handler.write_num_partitions == 4
    assert handler.teradata is io_manager.teradata

    from pyspark.sql import DataFrame as SparkDataFrame

    assert io_manager.default_load_type() is SparkDataFrame


def test_io_manager_defaults():
    io_manager = TeradataPySparkIOManager(teradata=make_resource())
    (handler,) = io_manager.type_handlers()
    assert handler.batch_size == DEFAULT_BATCH_SIZE
    assert handler.string_length == DEFAULT_STRING_LENGTH


def test_lazy_exports_available_from_package_root():
    assert dagster_teradata.TeradataPySparkIOManager is TeradataPySparkIOManager
    assert dagster_teradata.TeradataPySparkTypeHandler is TeradataPySparkTypeHandler
    assert "TeradataPySparkIOManager" in dir(dagster_teradata)


@pytest.mark.usefixtures("existing_table_schema_matches")
def test_handle_output_counts_before_write_without_recomputing(handler):
    """row_count must come from a cached count, not a second pass over the plan.

    Counting after .save() would re-execute the entire upstream lineage, and for a
    non-deterministic source could report a number that disagrees with what was
    actually written.
    """
    frame = make_frame(["a"])
    connection = MagicMock()

    metadata = handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    assert metadata["row_count"] == 2
    frame.cache.assert_called_once_with()
    frame.count.assert_called_once_with()
    frame.unpersist.assert_called_once_with()


@pytest.mark.usefixtures("existing_table_schema_matches")
def test_handle_output_unpersists_even_when_the_write_fails(handler):
    """A failed write must not leak the cached frame in Spark's storage memory."""
    frame = make_frame(["a"])
    frame.write.format.return_value.mode.return_value.options.return_value.save.side_effect = RuntimeError(
        "jdbc write failed"
    )

    with pytest.raises(RuntimeError, match="jdbc write failed"):
        handler.handle_output(MagicMock(), make_table_slice(), frame, MagicMock())

    frame.unpersist.assert_called_once_with()


@pytest.mark.usefixtures("existing_table_schema_matches")
def test_handle_output_unpersists_even_when_the_count_fails(handler):
    """count() runs against the cached frame, so it must be inside the same try."""
    frame = make_frame(["a"])
    frame.count.side_effect = RuntimeError("spark count failed")

    with pytest.raises(RuntimeError, match="spark count failed"):
        handler.handle_output(MagicMock(), make_table_slice(), frame, MagicMock())

    frame.unpersist.assert_called_once_with()


def test_read_partitioning_collision_is_detected_case_insensitively():
    # Spark resolves JDBC option names case-insensitively, so "URL" would still
    # replace the handler-selected endpoint once Spark normalizes the options.
    for key in ("URL", "DbTable", "USER"):
        handler = TeradataPySparkTypeHandler(
            make_resource(), read_partitioning={key: "override"}
        )
        with pytest.raises(ValueError, match="must not override the reserved options"):
            handler._read_options()


def test_column_types_override_cannot_smuggle_in_an_unwritable_type():
    # No Teradata column type can give Spark a JDBC mapping for void, so the
    # override must not bypass the up-front rejection.
    handler = TeradataPySparkTypeHandler(
        make_resource(), column_types={"col": "VARCHAR(50)"}
    )
    with pytest.raises(ValueError, match="type void"):
        handler.column_type(T.StructField("col", T.NullType()))


def test_column_types_override_cannot_smuggle_in_a_nested_type():
    handler = TeradataPySparkTypeHandler(
        make_resource(), column_types={"col": "VARCHAR(50)"}
    )
    with pytest.raises(ValueError, match="unsupported nested type"):
        handler.column_type(T.StructField("col", T.ArrayType(T.StringType())))


def test_handle_output_validates_schema_before_committing_cleanup(handler):
    """An unwritable field must not reach the commit that makes the DELETE final.

    _create_table_if_absent returns early when the table already exists, so
    without an explicit up-front check the failure would surface only inside
    Spark -- after the cleanup DELETE had been committed and the old rows lost.
    """
    frame = make_frame(
        ["ok", "bad"],
        schema_fields=[
            T.StructField("ok", T.LongType()),
            T.StructField("bad", T.NullType()),
        ],
    )
    connection = MagicMock()

    with pytest.raises(ValueError, match="type void"):
        handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    # Nothing was committed, so the DELETE rolls back with the transaction, and no
    # write was attempted.
    connection.commit.assert_not_called()
    frame.write.format.assert_not_called()


@pytest.mark.usefixtures("existing_table_schema_matches")
def test_handle_output_leaves_a_caller_cached_frame_persisted(handler):
    """Evicting a cache the caller owns would force it to recompute its lineage."""
    frame = make_frame(["a"])
    frame.is_cached = True

    handler.handle_output(MagicMock(), make_table_slice(), frame, MagicMock())

    frame.cache.assert_not_called()
    frame.unpersist.assert_not_called()


def test_string_length_above_teradata_varchar_limit_is_rejected():
    # Teradata caps UNICODE VARCHAR at 32000 characters; a wider value would only
    # be rejected by the database after the cleanup DELETE had been committed.
    with pytest.raises(ValueError, match="string_length must be at most 32000"):
        TeradataPySparkTypeHandler(make_resource(), string_length=32_001)


def test_string_length_at_the_teradata_varchar_limit_is_accepted():
    handler = TeradataPySparkTypeHandler(make_resource(), string_length=32_000)
    assert handler.column_type(T.StructField("c", T.StringType())) == (
        "VARCHAR(32000) CHARACTER SET UNICODE"
    )


def test_io_manager_rejects_string_length_above_the_varchar_limit():
    with pytest.raises(ValidationError):
        TeradataPySparkIOManager(teradata=make_resource(), string_length=32_001)


def test_partition_column_validation_accepts_a_differently_cased_column():
    # Teradata identifiers are case-insensitive, so "ID" selects the column "id".
    handler = TeradataPySparkTypeHandler(
        make_resource(),
        read_partitioning={
            "partitionColumn": "ID",
            "lowerBound": 0,
            "upperBound": 100,
            "numPartitions": 4,
        },
    )
    spark, reader = _spark_with_reader()
    table_slice = make_table_slice(columns=["id", "amount"])

    with patch(
        "dagster_teradata.pyspark_type_handler.SparkSession.getActiveSession",
        return_value=spark,
    ):
        handler.load_input(MagicMock(), table_slice, MagicMock())

    reader.load.assert_called_once_with()


def test_partition_column_validation_honours_spark_case_insensitive_option_names():
    # Spark honours "PartitionColumn", so skipping validation for it would let an
    # invalid column through to the JDBC read.
    handler = TeradataPySparkTypeHandler(
        make_resource(), read_partitioning={"PartitionColumn": "missing"}
    )
    spark, _ = _spark_with_reader()
    table_slice = make_table_slice(columns=["id", "amount"])

    with (
        patch(
            "dagster_teradata.pyspark_type_handler.SparkSession.getActiveSession",
            return_value=spark,
        ),
        pytest.raises(ValueError, match="is not among the loaded columns"),
    ):
        handler.load_input(MagicMock(), table_slice, MagicMock())


def test_column_types_override_cannot_smuggle_in_an_unsupported_scalar_type():
    # DayTimeIntervalType has no Teradata JDBC mapping, so no DDL override can
    # make it writable; without validating before the override it would reach the
    # commit and then fail inside Spark, after the old rows were deleted.
    handler = TeradataPySparkTypeHandler(
        make_resource(), column_types={"col": "VARCHAR(50)"}
    )
    with pytest.raises(ValueError, match="unsupported Spark type"):
        handler.column_type(T.StructField("col", T.DayTimeIntervalType()))


def test_handle_output_rejects_unsupported_scalar_before_committing_cleanup(handler):
    frame = make_frame(
        ["ok", "bad"],
        schema_fields=[
            T.StructField("ok", T.LongType()),
            T.StructField("bad", T.DayTimeIntervalType()),
        ],
    )
    connection = MagicMock()

    with pytest.raises(ValueError, match="unsupported Spark type"):
        handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    connection.commit.assert_not_called()
    frame.write.format.assert_not_called()


def test_column_types_override_still_applies_to_supported_types():
    # The validation must not break the documented purpose of column_types.
    handler = TeradataPySparkTypeHandler(
        make_resource(), column_types={"amount": "DECIMAL(18,4)"}
    )
    assert (
        handler.column_type(T.StructField("amount", T.DoubleType())) == "DECIMAL(18,4)"
    )


_DBC_ATTRS = {
    # canonical type -> (ColumnType, ColumnLength, TotalDigits, FracDigits, CharType)
    "BIGINT": ("I8", 8, None, None, 0),
    "INTEGER": ("I", 4, None, None, 0),
    "FLOAT": ("F", 8, None, None, 0),
    "DATE": ("DA", 4, None, None, 0),
    "BLOB": ("BO", 2097088000, None, None, 0),
    "VARCHAR(1024)": ("CV", 1024, None, None, 1),
    "VARCHAR(1024) UNICODE": ("CV", 2048, None, None, 2),
    "VARCHAR(50)": ("CV", 50, None, None, 1),
    "DECIMAL(5,3)": ("D", 4, 5, 3, 0),
    "TIMESTAMP(6)": ("TS", 26, None, 6, 0),
    "INTERVAL": ("YR", 2, None, None, 0),
}


def _cursor_with_columns(connection, columns):
    """Make the mocked connection's cursor report an existing table.

    ``columns`` is either a list of names (all typed BIGINT, matching make_frame's
    default LongType fields) or a mapping of name -> key into _DBC_ATTRS.
    """
    if not isinstance(columns, dict):
        columns = {name: "BIGINT" for name in columns}
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = [
        [name, *_DBC_ATTRS[type_key], "T"] for name, type_key in columns.items()
    ]
    return cursor


def test_handle_output_rejects_added_column_before_committing_cleanup(handler):
    # The table is created once and reused, so a new column would make Spark's
    # append fail inside the JVM -- after the cleanup DELETE had been committed.
    frame = make_frame(["a", "b"])
    connection = MagicMock()
    _cursor_with_columns(connection, ["a"])

    with pytest.raises(ValueError, match="does not match the existing table"):
        handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    connection.commit.assert_not_called()
    frame.write.format.assert_not_called()


def test_handle_output_rejects_renamed_column_before_committing_cleanup(handler):
    frame = make_frame(["a", "b_new"])
    connection = MagicMock()
    _cursor_with_columns(connection, ["a", "b_old"])

    with pytest.raises(ValueError, match="does not match the existing table"):
        handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    connection.commit.assert_not_called()


def test_handle_output_rejects_dropped_column_before_committing_cleanup(handler):
    frame = make_frame(["a"])
    connection = MagicMock()
    _cursor_with_columns(connection, ["a", "b"])

    with pytest.raises(ValueError, match="table columns not present in the DataFrame"):
        handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    connection.commit.assert_not_called()


def test_handle_output_accepts_matching_schema_ignoring_case(handler):
    # Teradata identifiers are case-insensitive, so differing case is not drift.
    frame = make_frame(["a", "b"])
    connection = MagicMock()
    _cursor_with_columns(connection, ["A", "B"])

    handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    connection.commit.assert_called_once()


def test_handle_output_renames_columns_to_the_tables_exact_case(handler):
    # Spark's JDBC append resolves DataFrame columns against the fetched table
    # schema with Spark's own analyzer, which is case-sensitive under
    # spark.sql.caseSensitive=true. A frame column that only differs from the
    # catalog's spelling by case must be renamed before the write, or the append
    # can fail inside Spark after the cleanup DELETE has already been committed.
    frame = make_frame(["a", "b"])
    connection = MagicMock()
    _cursor_with_columns(connection, ["A", "B"])

    handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    frame.select.assert_called_once()
    assert [call.args[0] for call in frame.__getitem__.call_args_list] == [
        "`a`",
        "`b`",
    ]
    assert [
        call.args[0] for call in frame.__getitem__.return_value.alias.call_args_list
    ] == ["A", "B"]


def test_handle_output_does_not_rename_columns_already_matching_the_table(handler):
    frame = make_frame(["a", "b"])
    connection = MagicMock()
    _cursor_with_columns(connection, ["a", "b"])

    handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    frame.select.assert_not_called()


def test_existing_table_is_rejected_when_dbc_is_not_readable(handler):
    # The table exists but its schema cannot be validated: appending blindly could
    # fail after the cleanup DELETE was committed, so the write is refused first.
    frame = make_frame(["a"])
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = (1,)
    cursor.fetchall.side_effect = teradatasql.DatabaseError("no rights")

    with pytest.raises(ValueError, match="Cannot read the column definitions"):
        handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    connection.commit.assert_not_called()
    frame.write.format.assert_not_called()


def test_handle_output_rejects_changed_column_type_before_committing_cleanup(handler):
    # The reviewer's scenario: an existing `id INTEGER` column rematerialized as a
    # string. Names still match, so only a type comparison catches it -- and it must
    # be caught before the cleanup DELETE is committed.
    frame = make_frame(["id"], schema_fields=[T.StructField("id", T.StringType())])
    connection = MagicMock()
    _cursor_with_columns(connection, {"id": "INTEGER"})

    with pytest.raises(ValueError, match="is INTEGER in the existing table"):
        handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    connection.commit.assert_not_called()
    frame.write.format.assert_not_called()


def test_handle_output_rejects_type_drift_introduced_by_column_types_override():
    # An override is the declared type, so drift must be judged against it too.
    handler = TeradataPySparkTypeHandler(
        make_resource(), column_types={"id": "VARCHAR(50)"}
    )
    frame = make_frame(["id"])
    connection = MagicMock()
    _cursor_with_columns(connection, {"id": "INTEGER"})

    with pytest.raises(ValueError, match="would store it as VARCHAR\\(50\\)"):
        handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    connection.commit.assert_not_called()


@pytest.mark.parametrize(
    "override",
    [
        "VARCHAR(10) CHARACTER SET UNICODE",
        "CHARACTER VARYING(10)",
        "CHAR VARYING(10) NOT CASESPECIFIC",
    ],
)
def test_qualified_character_override_drift_is_rejected_before_cleanup(override):
    # Widening a qualified/aliased override must be caught by the drift check:
    # otherwise the length check trusts the new width, the old rows are deleted and
    # the JDBC write fails against the table's real, narrower column.
    handler = TeradataPySparkTypeHandler(make_resource(), column_types={"id": override})
    frame = make_frame(["id"])
    connection = MagicMock()
    _cursor_with_columns(connection, {"id": "VARCHAR(50)"})

    with pytest.raises(ValueError, match="would store it as VARCHAR\\(10\\)"):
        handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    connection.commit.assert_not_called()


def test_handle_output_accepts_matching_column_types(handler):
    frame = make_frame(
        ["a", "b"],
        schema_fields=[
            T.StructField("a", T.LongType()),
            T.StructField("b", T.StringType()),
        ],
    )
    connection = MagicMock()
    _cursor_with_columns(connection, {"a": "BIGINT", "b": "VARCHAR(1024)"})

    handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    connection.commit.assert_called_once()


def test_unicode_varchar_length_is_compared_in_characters(handler):
    # DBC reports ColumnLength in bytes, so a UNICODE VARCHAR(1024) is 2048 there.
    # Treating that as drift would break every write to a UNICODE table.
    frame = make_frame(["s"], schema_fields=[T.StructField("s", T.StringType())])
    connection = MagicMock()
    _cursor_with_columns(connection, {"s": "VARCHAR(1024) UNICODE"})

    handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    connection.commit.assert_called_once()


def test_type_comparison_skipped_for_types_the_handler_cannot_declare(handler):
    # A hand-created INTERVAL column is not something this handler ever emits, so
    # it must be left alone rather than reported as a false mismatch.
    frame = make_frame(["a"])
    connection = MagicMock()
    _cursor_with_columns(connection, {"a": "INTERVAL"})

    handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    connection.commit.assert_called_once()


@pytest.mark.parametrize(
    ("declared", "expected"),
    [
        ("VARCHAR(50)", "VARCHAR(50)"),
        ("varchar(50)", "VARCHAR(50)"),
        ("DECIMAL(10, 2)", "DECIMAL(10,2)"),
        ("NUMERIC(10,2)", "DECIMAL(10,2)"),
        ("INT", "INTEGER"),
        ("INTEGER", "INTEGER"),
        ("TIMESTAMP(6)", "TIMESTAMP(6)"),
        ("TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP(6) WITH TIME ZONE"),
        ("BLOB", "BLOB"),
        ("CLOB", "CLOB"),
        # Sized LOBs keep their size; K/M/G are multiples of 1024.
        ("CLOB(1000)", "CLOB(1000)"),
        ("clob(100K)", "CLOB(102400)"),
        ("BLOB(2M)", "BLOB(2097152)"),
        # An explicit maximum is the same column as a bare declaration (LATIN or
        # BLOB bytes, and a UNICODE CLOB's characters).
        ("BLOB(2097088000)", "BLOB"),
        ("CLOB(2097088000)", "CLOB"),
        ("CLOB(1048544000)", "CLOB"),
        ("VARCHAR(10K)", None),
        # Not confidently parseable -> skipped rather than guessed at.
        ("VARCHAR(50) CHARACTER SET UNICODE", "VARCHAR(50)"),
        ("DECIMAL", None),
        ("NUMBER(10)", None),
        ("SYSUDTLIB.MY_UDT", None),
    ],
)
def test_canonical_teradata_type(declared, expected):
    assert _canonical_teradata_type(declared) == expected


# ColumnLength/CharType values verified against a live Teradata system.
@pytest.mark.parametrize(
    ("row", "expected"),
    [
        (("CO", 2097088000, None, None, 1), "CLOB"),  # CLOB (LATIN)
        (("CO", 2097088000, None, None, 2), "CLOB"),  # CLOB CHARACTER SET UNICODE
        (("CO", 100, None, None, 1), "CLOB(100)"),
        (("CO", 200, None, None, 2), "CLOB(100)"),  # UNICODE: 2 bytes per char
        (("CO", 1024, None, None, 1), "CLOB(1024)"),  # CLOB(1K)
        (("CO", 100, None, None, 3), None),  # unknown CharType
        # LATIN CLOB(1048544000) would read as a bare UNICODE CLOB: skipped.
        (("CO", 1048544000, None, None, 1), None),
        (("BO", 2097088000, None, None, 0), "BLOB"),
        (("BO", 100, None, None, 0), "BLOB(100)"),
        (("BO", 2097152, None, None, 0), "BLOB(2097152)"),  # BLOB(2M)
    ],
)
def test_catalog_type_preserves_lob_lengths(row, expected):
    from dagster_teradata._catalog import catalog_type

    assert catalog_type(row) == expected


@pytest.mark.parametrize(
    ("existing", "declared", "matches"),
    [
        ("CLOB(100)", "CLOB(1000)", False),
        ("CLOB(100)", "CLOB", False),
        ("CLOB", "CLOB(1000)", False),
        ("CLOB(100)", "CLOB(100)", True),
        ("CLOB", "CLOB", True),
        ("CLOB UNICODE", "CLOB(1048544000)", True),
        ("BLOB(100)", "BLOB", False),
    ],
)
def test_lob_size_drift_is_rejected_before_committing_cleanup(
    existing, declared, matches
):
    attrs = {
        "CLOB": ("CO", 2097088000, None, None, 1),
        "CLOB UNICODE": ("CO", 2097088000, None, None, 2),
        "CLOB(100)": ("CO", 100, None, None, 1),
        "BLOB(100)": ("BO", 100, None, None, 0),
    }[existing]
    field_type = T.BinaryType() if declared.startswith("BLOB") else T.StringType()
    handler = TeradataPySparkTypeHandler(make_resource(), column_types={"s": declared})
    frame = make_frame(["s"], [T.StructField("s", field_type)])
    connection = MagicMock()
    _cursor(connection).fetchall.return_value = [["s", *attrs, "T"]]

    if matches:
        handler.handle_output(MagicMock(), make_table_slice(), frame, connection)
        connection.commit.assert_called_once()
    else:
        with pytest.raises(ValueError, match="in the existing table"):
            handler.handle_output(MagicMock(), make_table_slice(), frame, connection)
        connection.commit.assert_not_called()
        frame.write.format.assert_not_called()


@pytest.mark.usefixtures("existing_table_schema_matches")
def test_lineage_failure_during_count_leaves_cleanup_uncommitted(handler):
    # count() is the first Spark action, so it is what evaluates the upstream
    # lineage. If that fails, the cleanup DELETE must still be uncommitted so
    # TeradataDbClient.connect() rolls it back and the previous rows survive.
    frame = make_frame(["a"])
    frame.count.side_effect = RuntimeError("upstream lineage exploded")
    connection = MagicMock()

    with pytest.raises(RuntimeError, match="upstream lineage exploded"):
        handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    connection.commit.assert_not_called()
    frame.write.format.assert_not_called()
    # The frame this handler cached must still be released.
    frame.unpersist.assert_called_once()


@pytest.mark.usefixtures("existing_table_schema_matches")
def test_frame_is_materialized_before_the_cleanup_is_committed(handler):
    # Ordering guard: count() (materialization) must precede commit(), which must
    # itself precede save() so the JDBC write is not blocked by the DELETE's lock.
    frame = make_frame(["a"])
    connection = MagicMock()
    calls = []
    frame.count.side_effect = lambda: calls.append("count") or 2
    connection.commit.side_effect = lambda: calls.append("commit")
    frame.write.format.return_value.mode.return_value.options.return_value.save.side_effect = (
        lambda: calls.append("save")
    )

    handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    assert calls == ["count", "commit", "save"]


# --------------------------------------------------------------------------------------
# review follow-ups
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "key", ["sessionInitStatement", "customSchema", "pushDownPredicate"]
)
def test_read_partitioning_rejects_options_outside_the_allowlist(key):
    # sessionInitStatement runs arbitrary SQL on every JDBC connection and
    # customSchema reinterprets column types: neither is a partitioning option.
    handler = TeradataPySparkTypeHandler(make_resource(), read_partitioning={key: "x"})
    with pytest.raises(ValueError, match="does not accept the options"):
        handler._read_options()


def test_read_partitioning_rejects_case_insensitive_duplicate_options():
    # Spark normalizes option names, so the partitionColumn validated by
    # load_input could otherwise differ from the one Spark actually uses.
    with pytest.raises(ValueError, match="more than once") as excinfo:
        TeradataPySparkTypeHandler(
            make_resource(),
            read_partitioning={
                "partitionColumn": "id",
                "PartitionColumn": "other",
                "numPartitions": 4,
            },
        )
    assert "['PartitionColumn', 'partitionColumn']" in str(excinfo.value)


def test_read_partitioning_allowlist_is_case_insensitive():
    handler = TeradataPySparkTypeHandler(
        make_resource(),
        read_partitioning={"FetchSize": 10_000, "querytimeout": 60},
    )
    options = handler._read_options()
    assert options["FetchSize"] == 10_000
    assert options["querytimeout"] == 60


@pytest.mark.parametrize(
    "partial",
    [
        {"partitionColumn": "id"},
        {"partitionColumn": "id", "lowerBound": 0, "upperBound": 10},
        {"lowerBound": 0, "upperBound": 10},
        {"numPartitions": 4},
        {"PARTITIONCOLUMN": "id", "lowerbound": 0, "UpperBound": 9, "fetchsize": 5},
    ],
)
def test_read_partitioning_requires_the_partitioning_group_together(partial):
    handler = TeradataPySparkTypeHandler(make_resource(), read_partitioning=partial)
    with pytest.raises(ValueError, match="together; missing"):
        handler._read_options()


def test_read_partitioning_accepts_the_full_group_in_any_case():
    handler = TeradataPySparkTypeHandler(
        make_resource(),
        read_partitioning={
            "PARTITIONCOLUMN": "id",
            "lowerbound": 0,
            "UpperBound": 9,
            "numPartitions": 2,
            "fetchsize": 5,
        },
    )
    assert handler._read_options()["numPartitions"] == 2


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("database", "analytics,LOGMECH=TD2"),
        ("host", "td.example.com/LOGMECH=TD2"),
        ("logmech", "LDAP,TMODE=TERA"),
        ("port", "1025,X=1"),
    ],
)
def test_jdbc_url_rejects_parameter_injection(field, value):
    kwargs = {"host": "td.example.com", "user": "dbc", "password": "dbc", field: value}
    handler = TeradataPySparkTypeHandler(TeradataResource(**kwargs))
    with pytest.raises(ValueError, match=f"'{field}' must not contain"):
        handler.jdbc_url()


def _connect_like(name: str) -> object:
    # Spark Connect classes live under pyspark.sql.connect in every PySpark
    # version; on 4.x they also subclass pyspark.sql.DataFrame/SparkSession.
    cls = type(name, (), {"__module__": "pyspark.sql.connect.dataframe"})
    return cls()


def test_handle_output_rejects_spark_connect_dataframe(handler):
    with pytest.raises(TypeError, match="does not support Spark Connect"):
        handler.handle_output(
            MagicMock(), make_table_slice(), _connect_like("DataFrame"), MagicMock()
        )


def test_load_input_rejects_spark_connect_session(handler):
    with (
        patch(
            "dagster_teradata.pyspark_type_handler.SparkSession.getActiveSession",
            return_value=_connect_like("SparkSession"),
        ),
        pytest.raises(TypeError, match="does not support Spark Connect"),
    ):
        handler.load_input(MagicMock(), make_table_slice(), MagicMock())


@pytest.mark.parametrize("lookup", ["active", "default"])
def test_load_input_rejects_connect_only_process(handler, lookup):
    # With only a Connect session, neither classic lookup finds anything, so the
    # generic "requires an active SparkSession" error must not win.
    import types

    session = _connect_like("SparkSession")

    class ConnectSparkSession:
        _default_session = session if lookup == "default" else None

        @staticmethod
        def getActiveSession():
            return session if lookup == "active" else None

    module = types.ModuleType("pyspark.sql.connect.session")
    module.SparkSession = ConnectSparkSession  # type: ignore[attr-defined]
    with (
        patch.dict("sys.modules", {"pyspark.sql.connect.session": module}),
        patch(
            "dagster_teradata.pyspark_type_handler.SparkSession.getActiveSession",
            return_value=None,
        ),
        patch(
            "dagster_teradata.pyspark_type_handler.SparkSession._instantiatedSession",
            None,
        ),
        pytest.raises(TypeError, match="does not support Spark Connect"),
    ):
        handler.load_input(MagicMock(), make_table_slice(), MagicMock())


def test_connect_dataframe_dispatches_to_handler_for_named_rejection(handler):
    # On PySpark 3.4/3.5 the Connect DataFrame is not a pyspark.sql.DataFrame, so
    # unless it is registered DbIOManager fails it as an unsupported type before
    # handle_output can explain that Spark Connect is the problem.
    import types

    from dagster._core.storage.db_io_manager import DbIOManager

    from dagster_teradata import pyspark_type_handler

    frame = _connect_like("DataFrame")
    module = types.ModuleType("pyspark.sql.connect.dataframe")
    module.DataFrame = type(frame)  # type: ignore[attr-defined]
    pyspark_type_handler._spark_connect_dataframe_type.cache_clear()
    try:
        with patch.object(
            pyspark_type_handler.importlib, "import_module", return_value=module
        ):
            db_io_manager = DbIOManager(
                type_handlers=[handler], db_client=MagicMock(), database="db"
            )
    finally:
        pyspark_type_handler._spark_connect_dataframe_type.cache_clear()

    db_io_manager._check_supported_type(type(frame))
    assert db_io_manager._resolve_handler(type(frame)) is handler
    with pytest.raises(TypeError, match="does not support Spark Connect"):
        handler.handle_output(MagicMock(), make_table_slice(), frame, MagicMock())


def test_handle_output_validates_jdbc_options_before_committing_cleanup():
    # An invalid resource setting must fail while the cleanup DELETE is still
    # uncommitted (so it rolls back), not after the early commit.
    resource = TeradataResource(
        host="td.example.com", user="dbc", password="dbc", database="analytics,x=1"
    )
    handler = TeradataPySparkTypeHandler(resource)
    frame = make_frame(["a"])
    connection = MagicMock()

    with pytest.raises(ValueError):
        handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    connection.commit.assert_not_called()
    frame.count.assert_not_called()
    frame.write.format.assert_not_called()


@pytest.mark.usefixtures("existing_table_schema_matches")
def test_handle_output_skips_the_write_for_an_empty_frame(handler):
    # Nothing to write, so no JDBC connections are opened and the cleanup DELETE
    # is not committed early: it stays in the I/O manager's transaction.
    frame = make_frame(["a"])
    frame.count.return_value = 0
    connection = MagicMock()
    _cursor(connection).fetchone.return_value = (1,)

    metadata = handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    assert metadata["row_count"] == 0
    frame.write.format.assert_not_called()
    connection.commit.assert_not_called()


@pytest.mark.usefixtures("existing_table_schema_matches")
def test_handle_output_fails_when_an_overlapping_run_duplicated_rows(handler):
    # The cleanup DELETE is committed before the Spark write, so an overlapping run
    # can append the same rows. The post-write count turns that into a failure.
    frame = make_frame(["a"])  # count() == 2
    connection = MagicMock()
    cursor = _cursor(connection)
    cursor.fetchone.side_effect = [(1,), (4,)]  # table exists; then 4 rows stored

    with pytest.raises(RuntimeError, match="overlapping"):
        handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    count_statement = _executed(cursor)[-1]
    assert count_statement == (
        'LOCKING TABLE "my_db"."my_table" FOR ACCESS '
        'SELECT COUNT(*) FROM "my_db"."my_table"'
    )


@pytest.mark.usefixtures("existing_table_schema_matches")
def test_overlap_check_tolerates_fewer_rows_in_the_slice(handler):
    # Rows outside a partition predicate are not counted, so fewer is not an error.
    frame = make_frame(["a"])
    connection = MagicMock()
    _cursor(connection).fetchone.side_effect = [(1,), (1,)]

    handler.handle_output(MagicMock(), make_table_slice(), frame, connection)


@pytest.mark.usefixtures("existing_table_schema_matches")
def test_overlap_check_is_advisory_when_the_count_query_fails(handler):
    frame = make_frame(["a"])
    connection = MagicMock()
    cursor = _cursor(connection)

    def execute(statement, *args, **kwargs):
        if statement.startswith("LOCKING TABLE"):
            raise teradatasql.DatabaseError("[Error 3523] no SELECT access")

    cursor.execute.side_effect = execute
    cursor.fetchone.return_value = (1,)

    metadata = handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    assert metadata["row_count"] == 2


def test_drift_check_is_skipped_for_a_table_this_call_created(handler):
    connection = MagicMock()
    cursor = _cursor(connection)
    cursor.fetchone.side_effect = [None, (2,)]  # table absent; then post-write count

    handler.handle_output(
        MagicMock(), make_table_slice(), make_frame(["a"]), connection
    )

    assert not any("DBC.ColumnsV" in stmt for stmt in _executed(cursor))


def test_drift_check_rejects_a_view_target_before_committing_cleanup(handler):
    # _table_exists() sees a view as an existing target, and delete_table_slice()
    # may have deleted through an updatable view; it must be rejected rather than
    # treated as a missing table and appended to after the commit.
    connection = MagicMock()
    cursor = _cursor(connection)
    cursor.fetchone.return_value = (1,)
    cursor.fetchall.return_value = [["a", None, None, None, None, None, "V"]]
    frame = make_frame(["a"])

    with pytest.raises(ValueError, match="not a base table"):
        handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    connection.commit.assert_not_called()
    frame.count.assert_not_called()


@pytest.mark.parametrize("kind", ["T", "O"])
def test_drift_check_accepts_base_tables(handler, kind):
    connection = MagicMock()
    cursor = _cursor(connection)
    cursor.fetchone.return_value = (1,)
    cursor.fetchall.return_value = [["a", "I8", 8, None, None, 0, kind]]

    handler.handle_output(
        MagicMock(), make_table_slice(), make_frame(["a"]), connection
    )

    connection.commit.assert_called_once()


@pytest.mark.usefixtures("existing_table_schema_matches")
def test_handle_output_leaves_a_plan_cached_through_another_object_persisted(handler):
    # Spark's cache is keyed by logical plan: the caller may have cached an equal
    # plan through a different DataFrame object, so is_cached on *this* object is
    # False while the cache manager still holds the entry.
    frame = make_frame(["a"])
    frame.is_cached = False
    frame.storageLevel = StorageLevel.MEMORY_AND_DISK

    handler.handle_output(MagicMock(), make_table_slice(), frame, MagicMock())

    frame.cache.assert_not_called()
    frame.unpersist.assert_not_called()


@pytest.mark.parametrize("char_type", [3, 4, 5])
def test_catalog_type_skips_char_types_it_cannot_convert(char_type):
    from dagster_teradata._catalog import catalog_type

    # GRAPHIC/KANJI types store several bytes per character at ratios this module
    # does not model; a guessed length would be reported as false drift.
    assert catalog_type(("CV", 100, None, None, char_type)) is None
    assert catalog_type(("CV", 100, None, None, 2)) == "VARCHAR(50)"
    assert catalog_type(("CV", 100, None, None, 1)) == "VARCHAR(100)"


def test_lazy_export_reports_a_too_old_requirement(monkeypatch):
    # pyspark 3.3 lacks TimestampNTZType, which the handler references at module
    # scope: that surfaces as AttributeError, not ImportError.
    import importlib
    import sys

    real_import_module = importlib.import_module

    def fake_import_module(name, *args, **kwargs):
        if name == "dagster_teradata.pyspark_type_handler":
            raise AttributeError("module 'pyspark.sql.types' has no attribute 'X'")
        return real_import_module(name, *args, **kwargs)

    monkeypatch.setattr(importlib, "import_module", fake_import_module)
    monkeypatch.delitem(sys.modules, "dagster_teradata.pyspark_type_handler")

    with pytest.raises(ImportError, match="likely older than dagster-teradata"):
        dagster_teradata.TeradataPySparkTypeHandler  # noqa: B018


def test_lazy_export_keeps_an_unrelated_import_error(monkeypatch):
    # pyspark itself imported fine, so a missing module elsewhere in the handler's
    # import chain must surface as-is, not as "pyspark is too old".
    import importlib
    import sys

    real_import_module = importlib.import_module

    def fake_import_module(name, *args, **kwargs):
        if name == "dagster_teradata.pyspark_type_handler":
            raise ModuleNotFoundError("No module named 'some_dependency'")
        return real_import_module(name, *args, **kwargs)

    monkeypatch.setattr(importlib, "import_module", fake_import_module)
    monkeypatch.delitem(sys.modules, "dagster_teradata.pyspark_type_handler")

    with pytest.raises(ImportError, match="some_dependency") as excinfo:
        dagster_teradata.TeradataPySparkTypeHandler  # noqa: B018
    assert "older than" not in str(excinfo.value)


def test_lazy_export_keeps_an_unrelated_attribute_error(monkeypatch):
    import importlib
    import sys

    real_import_module = importlib.import_module

    def fake_import_module(name, *args, **kwargs):
        if name == "dagster_teradata.pyspark_type_handler":
            raise AttributeError("coding bug", obj=object())
        return real_import_module(name, *args, **kwargs)

    monkeypatch.setattr(importlib, "import_module", fake_import_module)
    monkeypatch.delitem(sys.modules, "dagster_teradata.pyspark_type_handler")

    with pytest.raises(AttributeError, match="coding bug"):
        dagster_teradata.TeradataPySparkTypeHandler  # noqa: B018


def test_case_rename_quotes_column_names_with_dots_and_backticks(handler):
    # Spark parses an unquoted obj["a.b"] as a nested field reference, so the
    # rename projection must backtick-quote each name (doubling any backtick).
    frame = make_frame(["a.b", "c`d"])
    connection = MagicMock()
    _cursor_with_columns(connection, ["A.B", "C`D"])

    handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    assert [call.args[0] for call in frame.__getitem__.call_args_list] == [
        "`a.b`",
        "`c``d`",
    ]
    assert [
        call.args[0] for call in frame.__getitem__.return_value.alias.call_args_list
    ] == ["A.B", "C`D"]


def test_drift_check_keeps_leading_blanks_in_column_names(handler):
    # DBC pads names on the right only; a quoted " id" must not reflect as "id",
    # or its second materialization would be reported as schema drift.
    connection = MagicMock()
    cursor = _cursor(connection)
    cursor.fetchone.return_value = (1,)
    cursor.fetchall.return_value = [[" id   ", "I8", 8, None, None, 0, "T"]]

    handler.handle_output(
        MagicMock(), make_table_slice(), make_frame([" id"]), connection
    )

    connection.commit.assert_called_once()


def _existing_varchar_table(connection: MagicMock, length: int = 1024) -> MagicMock:
    cursor = _cursor(connection)
    cursor.fetchone.return_value = (1,)
    cursor.fetchall.return_value = [
        ["a", "I8", 8, None, None, 0, "T"],
        ["b", "CV", length, None, None, 1, "T"],
    ]
    return cursor


def _string_frame() -> MagicMock:
    return make_frame(
        ["a", "b"],
        [T.StructField("a", T.LongType()), T.StructField("b", T.StringType())],
    )


def test_overlength_string_is_rejected_before_cleanup_is_committed(
    handler, string_lengths
):
    # Teradata would reject the value inside .save(), after the cleanup DELETE
    # had been committed, so the previous rows must still be intact when it fails.
    string_lengths.side_effect = lambda frame, names: {"b": 1025}
    connection = MagicMock()
    cursor = _existing_varchar_table(connection)
    frame = _string_frame()

    with pytest.raises(
        ValueError, match="'b' has a 1025-character value but holds 1024"
    ):
        handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    string_lengths.assert_called_once_with(frame, ["b"])
    connection.commit.assert_not_called()
    frame.write.format.assert_not_called()
    # The DELETE issued by delete_table_slice() was rolled back and not re-issued.
    connection.rollback.assert_called_once()
    assert not any(stmt.startswith("DELETE") for stmt in _executed(cursor))
    frame.unpersist.assert_called_once()


def test_string_at_declared_length_is_written(handler, string_lengths):
    string_lengths.side_effect = lambda frame, names: {"b": 1024}
    connection = MagicMock()
    _existing_varchar_table(connection)
    frame = _string_frame()

    handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    frame.write.format.return_value.mode.return_value.options.return_value.save.assert_called_once()


def test_string_length_check_uses_column_types_override(string_lengths):
    string_lengths.side_effect = lambda frame, names: {"b": 11}
    handler = TeradataPySparkTypeHandler(
        make_resource(), column_types={"b": "CHAR(10)"}
    )
    connection = MagicMock()
    _cursor(connection).fetchone.return_value = None
    frame = _string_frame()

    with pytest.raises(ValueError, match="'b' has a 11-character value but holds 10"):
        handler.handle_output(MagicMock(), make_table_slice(), frame, connection)


@pytest.mark.parametrize(
    "override", ["CLOB", "CLOB CHARACTER SET UNICODE", "JSON(1000)"]
)
def test_string_length_check_skips_unbounded_or_non_character_types(
    override, string_lengths
):
    handler = TeradataPySparkTypeHandler(make_resource(), column_types={"b": override})
    connection = MagicMock()
    _cursor(connection).fetchone.return_value = None

    handler.handle_output(MagicMock(), make_table_slice(), _string_frame(), connection)

    string_lengths.assert_not_called()


@pytest.mark.parametrize(
    ("override", "limit"),
    [
        ("VARCHAR(10)", 10),
        ("varchar ( 10 )", 10),
        ("VARCHAR(10) CHARACTER SET UNICODE", 10),
        ("VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC", 10),
        ("CHARACTER VARYING(10)", 10),
        ("CHAR VARYING(10)", 10),
        ("CHARACTER(10)", 10),
        ("CHAR(10) CHARACTER SET UNICODE", 10),
        ("CHAR", 1),
        ("CHARACTER CHARACTER SET UNICODE", 1),
        ("LONG VARCHAR CHARACTER SET LATIN", 64_000),
        ("LONG VARCHAR CHARACTER SET UNICODE", 32_000),
        ("CLOB(10)", 10),
        ("CLOB(2K) CHARACTER SET UNICODE", 2048),
        ("CHARACTER LARGE OBJECT(1M)", 1024**2),
    ],
)
def test_string_length_check_understands_every_character_spelling(
    override, limit, string_lengths
):
    # Any spelling or qualifier that went unrecognized would skip the check and
    # let an over-length value fail only after the cleanup DELETE is committed.
    string_lengths.side_effect = lambda frame, names: {"b": limit + 1}
    handler = TeradataPySparkTypeHandler(make_resource(), column_types={"b": override})
    connection = MagicMock()
    cursor = _cursor(connection)
    cursor.fetchone.return_value = None
    frame = _string_frame()

    with pytest.raises(
        ValueError, match=rf"'b' has a {limit + 1}-character value but holds {limit}\."
    ):
        handler.handle_output(MagicMock(), make_table_slice(), frame, connection)
    # Only the new table's DDL was committed; the cleanup was never re-issued.
    assert not any(stmt.startswith("DELETE") for stmt in _executed(cursor))
    frame.write.format.assert_not_called()


@pytest.mark.parametrize(
    "override",
    [
        "VARCHAR",
        "CHAR VARYING",
        "VARCHAR(10K)",
        "LONG VARCHAR",
        "LONG VARCHAR CHARACTER SET KANJISJIS",
        "VARCHAR(10) COMPRESS",
    ],
)
def test_string_length_check_rejects_character_types_it_cannot_size(
    override, string_lengths
):
    handler = TeradataPySparkTypeHandler(make_resource(), column_types={"b": override})
    connection = MagicMock()
    cursor = _cursor(connection)
    cursor.fetchone.return_value = None
    frame = _string_frame()

    with pytest.raises(ValueError, match="Cannot determine the capacity of column 'b'"):
        handler.handle_output(MagicMock(), make_table_slice(), frame, connection)
    # Rejected before any DDL, so no table is created with the unusable type.
    cursor.execute.assert_not_called()
    connection.commit.assert_not_called()
    frame.write.format.assert_not_called()


def test_string_length_check_uses_the_table_spelling_after_rename(
    handler, string_lengths
):
    # The frame is renamed to the catalog's spelling before it is cached, so
    # the aggregation must name the renamed column, while the declared type is
    # still looked up under the frame's own spelling.
    string_lengths.side_effect = lambda frame, names: {"B": 1025}
    connection = MagicMock()
    cursor = _cursor(connection)
    cursor.fetchone.return_value = (1,)
    cursor.fetchall.return_value = [["B", "CV", 1024, None, None, 1, "T"]]
    frame = make_frame(["b"], [T.StructField("b", T.StringType())])
    # Like Spark, the rename yields a frame whose schema has the new spelling.
    renamed = make_frame(["B"], [T.StructField("B", T.StringType())])
    frame.select.return_value = renamed

    with pytest.raises(ValueError, match="'B' has a 1025-character value"):
        handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    string_lengths.assert_called_once_with(renamed, ["B"])


@pytest.mark.parametrize(
    "override",
    [
        "VARCHAR(10) NOT NULL",
        "integer not null",
        "INTEGER PRIMARY KEY",
        "INTEGER UNIQUE",
        "INTEGER CHECK (a > 0)",
        "INTEGER REFERENCES other (id)",
        "BIGINT GENERATED ALWAYS AS IDENTITY",
        "integer generated by default as identity (start with 1)",
    ],
)
def test_column_types_override_with_a_constraint_is_rejected_before_any_ddl(override):
    # A constraint is not validated against the frame, so a violating value would
    # fail inside the JDBC write after the cleanup DELETE had been committed.
    handler = TeradataPySparkTypeHandler(make_resource(), column_types={"a": override})
    frame = make_frame(["a"])
    connection = MagicMock()
    cursor = _cursor(connection)

    with pytest.raises(ValueError, match="constraint"):
        handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    cursor.execute.assert_not_called()
    connection.commit.assert_not_called()


def test_column_types_override_may_still_say_null_or_casespecific():
    handler = TeradataPySparkTypeHandler(
        make_resource(), column_types={"a": "VARCHAR(10) NOT CASESPECIFIC NULL"}
    )
    assert (
        handler.column_type(T.StructField("a", T.StringType()))
        == "VARCHAR(10) NOT CASESPECIFIC NULL"
    )


def test_float_columns_have_nan_replaced_by_null_before_the_write(handler):
    # Teradata FLOAT cannot hold NaN; the JDBC write would fail after the cleanup
    # DELETE was committed, so only float/double columns are wrapped, and the
    # rename to the table's spelling still applies to every column.
    frame = make_frame(
        ["f", "d", "i"],
        schema_fields=[
            T.StructField("f", T.FloatType()),
            T.StructField("d", T.DoubleType()),
            T.StructField("i", T.LongType()),
        ],
    )
    connection = MagicMock()
    _cursor(connection).fetchone.return_value = None
    projected = []

    def project(column, is_float):
        projected.append(is_float)
        return column

    with patch.object(
        TeradataPySparkTypeHandler, "_project_column", side_effect=project
    ):
        handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    assert projected == [True, True, False]
    frame.select.assert_called_once()


def test_frames_without_float_columns_are_not_reprojected(handler):
    frame = make_frame(["a"])
    connection = MagicMock()
    _cursor(connection).fetchone.return_value = None

    handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    frame.select.assert_not_called()


# --------------------------------------------------------------------------------------
# LATIN representability and executor time zones
# --------------------------------------------------------------------------------------


def test_generated_string_columns_are_unicode_not_the_user_default(handler):
    # A bare VARCHAR takes the user's default character set, which may be LATIN.
    assert "CHARACTER SET UNICODE" in handler.column_type(
        T.StructField("s", T.StringType())
    )


def test_unrepresentable_latin_values_are_rejected_before_cleanup_is_committed(
    handler,
):
    connection = MagicMock()
    cursor = _existing_varchar_table(connection)
    frame = _string_frame()

    with (
        patch.object(
            TeradataPySparkTypeHandler,
            "_latin_unrepresentable_counts",
            return_value={"b": 2},
        ) as counts,
        pytest.raises(ValueError, match="'b' has 2 value\\(s\\) with characters"),
    ):
        handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    counts.assert_called_once_with(frame, ["b"])
    connection.commit.assert_not_called()
    frame.write.format.assert_not_called()
    assert not any(stmt.startswith("DELETE") for stmt in _executed(cursor))


def test_latin_columns_come_from_the_catalog():
    cursor = MagicMock()
    cursor.fetchall.return_value = [["B   "]]
    schema = T.StructType(
        [T.StructField("a", T.StringType()), T.StructField("b", T.StringType())]
    )
    types = {"a": "VARCHAR(10)", "b": "VARCHAR(10)"}

    targets = TeradataPySparkTypeHandler._latin_string_columns(
        cursor, make_table_slice(), schema, types
    )

    assert targets == ["b"]
    assert "CharType = 1" in cursor.execute.call_args.args[0]


def test_latin_columns_fall_back_to_the_declared_type_without_dbc_access():
    # Without the catalog, a declaration with no character set may have taken a
    # LATIN user default, so it is checked too; only explicit UNICODE is skipped.
    cursor = MagicMock()
    cursor.execute.side_effect = teradatasql.DatabaseError("no access")
    schema = T.StructType(
        [
            T.StructField("a", T.StringType()),
            T.StructField("b", T.StringType()),
            T.StructField("c", T.StringType()),
            T.StructField("d", T.StringType()),
        ]
    )
    types = {
        "a": "VARCHAR(10) CHARACTER SET LATIN",
        "b": "VARCHAR(10)",
        "c": "VARCHAR(10) CHARACTER SET UNICODE",
        "d": "CLOB",
    }

    targets = TeradataPySparkTypeHandler._latin_string_columns(
        cursor, make_table_slice(), schema, types
    )

    assert targets == ["a", "b", "d"]


def test_latin_check_skips_frames_without_string_columns():
    cursor = MagicMock()
    schema = T.StructType([T.StructField("a", T.LongType())])

    targets = TeradataPySparkTypeHandler._latin_string_columns(
        cursor, make_table_slice(), schema, {"a": "BIGINT"}
    )

    assert targets == []
    cursor.execute.assert_not_called()


def _temporal_frame(master: str, conf: dict[str, str] | None = None) -> MagicMock:
    frame = make_frame(
        ["a", "ts"],
        [T.StructField("a", T.LongType()), T.StructField("ts", T.TimestampType())],
    )
    spark = frame.sparkSession
    spark._jvm.java.util.TimeZone.getDefault.return_value.hasSameRules.return_value = (
        True
    )
    utc_rules = {"UTC", "GMT", "Etc/UTC"}

    def get_time_zone(zone):
        rules = MagicMock()
        rules.hasSameRules.return_value = zone in utc_rules
        return rules

    spark._jvm.java.util.TimeZone.getTimeZone.side_effect = get_time_zone
    spark.sparkContext.master = master
    settings = conf or {}
    spark.sparkContext.getConf.return_value.get.side_effect = lambda key, default=None: (
        settings.get(key, default)
    )
    return frame


@pytest.mark.parametrize(
    "conf, expected",
    [
        ({}, None),
        ({"spark.executor.extraJavaOptions": "-Xmx2g -Duser.timezone=UTC"}, "UTC"),
        ({"spark.executor.extraJavaOptions": "-Duser.timezone='Etc/UTC'"}, "Etc/UTC"),
        (
            {
                "spark.executor.defaultJavaOptions": "-Duser.timezone=UTC",
                "spark.executor.extraJavaOptions": "-Duser.timezone=Asia/Kolkata",
            },
            "Asia/Kolkata",
        ),
        (
            {
                "spark.executor.extraJavaOptions": "-Duser.timezone=Asia/Kolkata",
                "spark.executorEnv._JAVA_OPTIONS": "-Duser.timezone=UTC",
            },
            "UTC",
        ),
        (
            {
                "spark.executorEnv.JAVA_TOOL_OPTIONS": "-Duser.timezone=UTC",
                "spark.executor.extraJavaOptions": "-Dfoo=-Duser.timezone=UTC",
            },
            "UTC",
        ),
        ({"spark.executor.extraJavaOptions": "-Dx.user.timezone=UTC"}, None),
    ],
)
def test_configured_executor_timezone(conf, expected):
    assert _configured_executor_timezone(conf) == expected


@pytest.mark.parametrize(
    "conf, message",
    [
        ({}, "none is configured"),
        (
            {"spark.executor.extraJavaOptions": "-Duser.timezone=Asia/Kolkata"},
            "configured as 'Asia/Kolkata'",
        ),
    ],
)
def test_non_utc_executor_timezone_is_rejected_before_cleanup_is_committed(
    handler, conf, message
):
    connection = MagicMock()
    _cursor(connection).fetchone.return_value = None
    frame = _temporal_frame("yarn", conf)

    with pytest.raises(ValueError, match=rf"executor JVM.*{message}"):
        handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    connection.commit.assert_not_called()


@pytest.mark.parametrize("zone", ["UTC", "GMT", "Etc/UTC"])
def test_utc_executor_configuration_passes_the_timezone_check(handler, zone):
    connection = MagicMock()
    _cursor(connection).fetchone.return_value = None
    frame = _temporal_frame(
        "spark://master:7077",
        {"spark.executor.extraJavaOptions": f"-Duser.timezone={zone}"},
    )

    handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    connection.commit.assert_called()


@pytest.mark.parametrize("master", ["local", "local[*]", "local[4, 2]"])
def test_local_mode_does_not_require_executor_configuration(handler, master):
    connection = MagicMock()
    _cursor(connection).fetchone.return_value = None
    frame = _temporal_frame(master)

    handler.handle_output(MagicMock(), make_table_slice(), frame, connection)

    frame.sparkSession.sparkContext.getConf.assert_not_called()

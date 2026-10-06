"""PySpark type handler for :py:class:`~dagster_teradata.TeradataIOManager`.

This module imports pyspark at module scope. It is deliberately *not* imported by
``dagster_teradata/__init__.py`` at import time so that pyspark stays an optional
dependency; the package exposes the names below lazily instead.

Install the optional dependency with::

    pip install "dagster-teradata[pyspark]"

Data is moved between Spark and Teradata over JDBC, so the Teradata JDBC driver
(``terajdbc4.jar``, ``com.teradata.jdbc.TeraDriver``) must be on the Spark
classpath, for example via ``spark.jars`` or ``spark-submit --jars``.

Only classic (JVM-backed) Spark sessions are supported. Spark Connect sessions
have no driver JVM for the handler to inspect and are rejected with a named error.
"""

from collections.abc import Mapping, Sequence
import functools
import importlib
import re
import sys
from typing import Any

import teradatasql
from dagster import (
    InputContext,
    MetadataValue,
    OutputContext,
    TableColumn,
    TableSchema,
    get_dagster_logger,
)
from dagster._core.storage.db_io_manager import DbTypeHandler, TableSlice
from pydantic import Field
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from dagster_teradata._catalog import (
    latin_columns,
    parse_character_type,
    validate_schema_matches_table,
)
from dagster_teradata._type_handler_base import (
    MAX_DECIMAL_PRECISION as _MAX_DECIMAL_PRECISION,
    MAX_VARCHAR_LENGTH as _MAX_VARCHAR_LENGTH,
    TeradataTableTypeHandler,
    case_insensitive_duplicates,
)
from dagster_teradata.io_manager import TeradataDbClient, TeradataIOManager
from dagster_teradata.resources import TeradataResource

# Teradata JDBC driver class; must be available on the Spark classpath.
_JDBC_DRIVER = "com.teradata.jdbc.TeraDriver"

# Rows the JDBC driver batches per insert round-trip during writes.
DEFAULT_BATCH_SIZE = 1_000

# Spark schemas carry no string length, so StringType columns default to this
# VARCHAR width. Override per column with column_types. The pandas and polars
# handlers fall back to CLOB past Teradata's VARCHAR limit, but there is nothing to
# measure here, so string_length is validated against that limit instead.
DEFAULT_STRING_LENGTH = 1024

_SIMPLE_TYPE_MAPPING = {
    T.ByteType: "BYTEINT",
    T.ShortType: "SMALLINT",
    T.IntegerType: "INTEGER",
    T.LongType: "BIGINT",
    T.FloatType: "FLOAT",
    T.DoubleType: "FLOAT",
    T.BooleanType: "BYTEINT",
    T.DateType: "DATE",
    T.TimestampType: "TIMESTAMP(6)",
    # T.TimestampNTZType only exists from PySpark 3.4, which is why the pyspark
    # extra floors at >=3.4: referencing it here raises AttributeError at import
    # time on 3.3, which dagster_teradata.__getattr__ turns into a named error.
    T.TimestampNTZType: "TIMESTAMP(6)",
    T.BinaryType: "BLOB",
}

# Characters with a meaning in the parameter section of a Teradata JDBC URL
# (jdbc:teradata://host/KEY=value,KEY=value). A resource value containing any of
# them could inject extra connection parameters, e.g. database="db,LOGMECH=TD2".
_JDBC_URL_SEPARATORS = (",", "=", "/")

# Where an executor JVM's options come from, in the order the JVM applies them, so
# the last -Duser.timezone found is the one in effect: JAVA_TOOL_OPTIONS first,
# then the command line (Spark's defaultJavaOptions before extraJavaOptions), and
# _JAVA_OPTIONS last.
_EXECUTOR_JAVA_OPTION_SOURCES = (
    "spark.executorEnv.JAVA_TOOL_OPTIONS",
    "spark.executor.defaultJavaOptions",
    "spark.executor.extraJavaOptions",
    "spark.executorEnv._JAVA_OPTIONS",
)
_USER_TIMEZONE_OPTION = re.compile(r"(?:^|\s)-Duser\.timezone=(?P<zone>\S+)")


def _configured_executor_timezone(conf: Any) -> str | None:
    """The ``-Duser.timezone`` the Spark configuration gives every executor JVM."""
    zone = None
    for key in _EXECUTOR_JAVA_OPTION_SOURCES:
        for match in _USER_TIMEZONE_OPTION.finditer(conf.get(key, None) or ""):
            zone = match["zone"].strip("'\"")
    return zone


# TeradataResource settings the teradatasql connection honours that have a
# same-meaning Teradata JDBC URL parameter. Without them the JDBC reads and writes
# would silently connect with the driver's defaults -- e.g. without the TLS
# certificate verification an sslmode of VERIFY-FULL demands.
_FORWARDED_JDBC_PARAMETERS = (
    ("sslmode", "SSLMODE"),
    ("sslca", "SSLCA"),
    ("sslcapath", "SSLCAPATH"),
    ("sslcrc", "SSLCRC"),
    ("sslcipher", "SSLCIPHER"),
    ("sslprotocol", "SSLPROTOCOL"),
    ("slcrl", "SSLCRL"),
    ("sslocsp", "SSLOCSP"),
    ("oidc_sslmode", "OIDC_SSLMODE"),
    ("https_proxy", "HTTPS_PROXY"),
    ("https_proxy_user", "HTTPS_PROXY_USER"),
    ("https_proxy_password", "HTTPS_PROXY_PASSWORD"),
    ("proxy_bypass_hosts", "PROXY_BYPASS_HOSTS"),
)
# Only meaningful, and only forwarded by the resource, with LOGMECH=BROWSER.
_FORWARDED_BROWSER_JDBC_PARAMETERS = (
    ("browser", "BROWSER"),
    ("browser_tab_timeout", "BROWSER_TAB_TIMEOUT"),
    ("browser_timeout", "BROWSER_TIMEOUT"),
)
# Resource settings the Teradata JDBC driver has no parameter for (it validates
# parameter names and rejects unknown ones, so they cannot be passed through).
_UNFORWARDABLE_JDBC_SETTINGS = ("http_proxy", "http_proxy_user", "http_proxy_password")


def _jdbc_url_value(value: Any) -> str:
    """Render a resource value as a Teradata JDBC URL parameter value.

    Booleans become ON/OFF. A value containing a separator, quote or whitespace is
    single-quoted with embedded quotes doubled, as the Teradata JDBC driver
    requires, so it cannot inject further parameters.
    """
    if isinstance(value, bool):
        return "ON" if value else "OFF"
    text = str(value)
    if any(char in text for char in (*_JDBC_URL_SEPARATORS, "'")) or any(
        char.isspace() for char in text
    ):
        return "'" + text.replace("'", "''") + "'"
    return text


# The only spark.read.jdbc options read_partitioning may set, casefolded because
# Spark resolves JDBC option names case-insensitively. This is an allowlist rather
# than a denylist: options such as sessionInitStatement (arbitrary SQL on every
# JDBC connection) or customSchema (reinterpreted column types) must not be
# reachable through a setting documented as tuning partitioned reads.
_READ_PARTITIONING_OPTIONS = {
    option.lower(): option
    for option in (
        "partitionColumn",
        "lowerBound",
        "upperBound",
        "numPartitions",
        "fetchsize",
        "queryTimeout",
    )
}
# Options that configure a partitioned read and must be set together.
_PARTITIONED_READ_GROUP = (
    "partitioncolumn",
    "lowerbound",
    "upperbound",
    "numpartitions",
)


def _quote_spark_name(name: str) -> str:
    """Backtick-quote a column name so Spark does not parse dots in it."""
    return "`" + name.replace("`", "``") + "`"


# Column constraints a column_types override could smuggle in after the type, plus
# identity columns: Spark lists every column in its INSERT, and Teradata rejects an
# explicit value for a GENERATED ALWAYS column -- after the cleanup was committed.
_COLUMN_CONSTRAINT = re.compile(
    r"\b(?:NOT\s+NULL|PRIMARY|UNIQUE|CHECK|REFERENCES|FOREIGN|GENERATED|IDENTITY)\b",
    re.IGNORECASE,
)


def _is_spark_connect(obj: Any) -> bool:
    """Whether ``obj`` is a Spark Connect DataFrame or session.

    On PySpark 3.4/3.5 Connect objects are separate classes; on 4.x classic and
    Connect DataFrames share the ``pyspark.sql.DataFrame`` base, so ``isinstance``
    alone cannot tell them apart. Their implementations live under
    ``pyspark.sql.connect`` in every version.
    """
    return type(obj).__module__.startswith("pyspark.sql.connect")


def _reject_spark_connect(obj: Any) -> None:
    if _is_spark_connect(obj):
        raise TypeError(
            "TeradataPySparkTypeHandler does not support Spark Connect: it needs a "
            "classic, JVM-backed SparkSession to check the driver JVM's time zone "
            "and to run JDBC reads and writes with the Teradata driver. Build the "
            "session without a remote (no .remote(...) and no SPARK_REMOTE)."
        )


def _active_spark_connect_session() -> Any:
    """The active or default Spark Connect session, if one exists.

    Read from ``sys.modules`` rather than imported: a Connect session can only
    exist once its module has been imported, and importing it here would pull in
    Connect's optional dependencies (grpc, pyarrow) just to find nothing.
    """
    module = sys.modules.get("pyspark.sql.connect.session")
    connect_session_type = getattr(module, "SparkSession", None)
    if connect_session_type is None:
        return None
    return connect_session_type.getActiveSession() or getattr(
        connect_session_type, "_default_session", None
    )


@functools.cache
def _spark_connect_dataframe_type() -> type | None:
    """The Spark Connect DataFrame class, when it is a separate class (3.4/3.5).

    Registered as a supported type only so that DbIOManager dispatches Connect
    DataFrames to handle_output, which rejects them with a named error, instead
    of failing them as an unsupported type. Returns None when Connect's optional
    dependencies are missing: then no Connect DataFrame can exist.
    """
    try:
        module = importlib.import_module("pyspark.sql.connect.dataframe")
    except Exception:  # noqa: BLE001 - any import failure means Connect is unusable
        return None
    frame_type = getattr(module, "DataFrame", None)
    if not isinstance(frame_type, type) or issubclass(frame_type, SparkDataFrame):
        return None
    return frame_type


def _is_persisted(frame: SparkDataFrame) -> bool:
    """Whether Spark's cache already holds this frame's plan.

    ``DataFrame.is_cached`` is per-*object* state, but Spark's cache is keyed by
    logical plan: two DataFrame objects built from an identical plan share one
    cache entry. ``storageLevel`` asks the cache manager about the plan itself.
    """
    level = frame.storageLevel
    return bool(frame.is_cached or level.useMemory or level.useDisk or level.useOffHeap)


class TeradataPySparkTypeHandler(TeradataTableTypeHandler[SparkDataFrame]):
    """Stores and loads PySpark DataFrames as Teradata tables over JDBC.

    Tables are created on first materialization from the DataFrame's schema and
    reused afterwards. Data itself moves through the Teradata JDBC driver
    (``df.write.jdbc`` / ``spark.read.jdbc``), so reads and writes can be
    parallelized across Spark executors.

    Because JDBC writes run on Spark's own sessions, separate from the connection
    the I/O manager uses for cleanup and DDL, the cleanup ``DELETE`` is committed
    before the write starts. Holding it open would deadlock: its Teradata WRITE
    lock is incompatible with the one the JDBC ``INSERT``\\ s need, and it is only
    released once ``handle_output`` returns. For the same reason the DataFrame is
    materialized (cached and counted) before the ``DELETE`` takes its lock, so
    its lineage may read the target table itself. As a consequence the cleanup and the
    write are not atomic - a failed write leaves the deleted rows gone and the
    table possibly partially loaded. Re-materializing the asset is always correct
    (the ``DELETE`` simply runs again), but keep this in mind for concurrent
    readers.

    That commit also releases the lock that would otherwise serialize runs, so
    **overlapping materializations of the same asset are unsafe**: both can commit
    their cleanup ``DELETE`` before either Spark write begins, and both then
    append, duplicating rows (the table is created ``NO PRIMARY INDEX``, so nothing
    rejects them). After every write the handler counts the rows in the target
    slice and fails the run if there are more than it wrote, so a duplication is
    surfaced rather than silent - but it cannot undo it. Serialize
    materializations of a given asset with a Dagster concurrency pool or run-queue
    tag limit, or write each run to its own staging table and swap it in yourself.
    The pandas and polars handlers do not have this problem: they insert on the
    same connection as the cleanup, so the lock is held for the whole
    materialization.

    The Teradata password is passed to Spark as the JDBC ``password`` option.
    Spark redacts data source options matching ``spark.redaction.regex`` (default
    ``(?i)secret|password|token``) in addition to
    ``spark.sql.redaction.options.regex`` before they reach query plans, the
    Spark UI and the event log. If you override ``spark.redaction.regex``, keep
    ``password`` in it.

    The JDBC URL carries the resource's database, port, logmech, TLS (``sslmode``,
    ``sslca``, ...), HTTPS proxy and, with ``logmech="browser"``, browser settings
    - so a proxy password set on the resource is part of the ``url`` option,
    which ``spark.sql.redaction.options.regex`` (default ``(?i)url``) redacts.
    ``http_proxy*`` settings have no Teradata JDBC equivalent and are not applied
    to the JDBC connections (a warning is logged). The resource's query band is
    not applied to the JDBC connections either.

    Args:
        teradata (TeradataResource): Resource whose host/database/credentials are
            used to build the JDBC URL and connection properties.
        batch_size (int): Rows the JDBC driver batches per insert round-trip.
            Defaults to 1000.
        string_length (int): VARCHAR width used for Spark ``StringType`` columns,
            whose schema carries no length. Defaults to 1024. Columns wider than
            this must be pinned via ``column_types`` (e.g. to ``CLOB``); a longer
            value fails the run before the previous rows are deleted.
        column_types (Mapping[str, str] | None): Explicit Teradata column types,
            keyed by column name. Overrides inference entirely for those columns.
        read_partitioning (Mapping[str, Any] | None): Options forwarded to
            ``spark.read.jdbc`` for partitioned reads, e.g.
            ``{"partitionColumn": "id", "lowerBound": 0, "upperBound": 1000000,
            "numPartitions": 8}``. Only ``partitionColumn``, ``lowerBound``,
            ``upperBound``, ``numPartitions``, ``fetchsize`` and ``queryTimeout``
            are accepted. The partition column must be part of the loaded columns.
        write_num_partitions (int | None): If set, the DataFrame is repartitioned
            to this many partitions before the JDBC write so that many parallel
            connections load the data.

    Example:
        .. code-block:: python

            from dagster_teradata import TeradataIOManager, TeradataPySparkTypeHandler

            class MyIOManager(TeradataIOManager):
                def type_handlers(self):
                    return [TeradataPySparkTypeHandler(self.teradata)]
    """

    def __init__(
        self,
        teradata: TeradataResource,
        batch_size: int = DEFAULT_BATCH_SIZE,
        string_length: int = DEFAULT_STRING_LENGTH,
        column_types: Mapping[str, str] | None = None,
        read_partitioning: Mapping[str, Any] | None = None,
        write_num_partitions: int | None = None,
    ):
        if batch_size < 1:
            raise ValueError(f"batch_size must be at least 1, got {batch_size}.")
        if string_length < 1:
            raise ValueError(f"string_length must be at least 1, got {string_length}.")
        if string_length > _MAX_VARCHAR_LENGTH:
            # Caught here rather than by Teradata, which would only reject the
            # generated VARCHAR(...) after the cleanup DELETE had been committed.
            raise ValueError(
                f"string_length must be at most {_MAX_VARCHAR_LENGTH}, got "
                f"{string_length}: that is the widest VARCHAR Teradata supports for "
                "a UNICODE column. Use column_types to declare wider columns as CLOB."
            )
        if write_num_partitions is not None and write_num_partitions < 1:
            raise ValueError(
                f"write_num_partitions must be at least 1, got {write_num_partitions}."
            )
        self.teradata = teradata
        self.batch_size = batch_size
        self.string_length = string_length
        self.column_types = dict(column_types or {})
        self.read_partitioning = dict(read_partitioning or {})
        # Spark normalizes option names case-insensitively, so two spellings of one
        # option would let the value validated here differ from the one Spark uses.
        duplicate_options = sorted(
            {
                key
                for key in self.read_partitioning
                if sum(k.lower() == key.lower() for k in self.read_partitioning) > 1
            }
        )
        if duplicate_options:
            raise ValueError(
                f"read_partitioning sets the same option more than once: "
                f"{duplicate_options}. Spark matches option names "
                "case-insensitively; keep a single spelling of each option."
            )
        self.write_num_partitions = write_num_partitions

    def jdbc_url(self) -> str:
        """JDBC URL for the Teradata system configured on the resource."""
        host = self.teradata.host
        if not host:
            raise ValueError(
                "TeradataPySparkTypeHandler requires the TeradataResource 'host' to "
                "build the JDBC URL."
            )
        for field, value in (
            ("host", host),
            ("database", self.teradata.database),
            ("port", self.teradata.port),
            ("logmech", self.teradata.logmech),
        ):
            if value and any(sep in str(value) for sep in _JDBC_URL_SEPARATORS):
                raise ValueError(
                    f"TeradataResource '{field}' must not contain any of "
                    f"{list(_JDBC_URL_SEPARATORS)}: they separate parameters in the "
                    "Teradata JDBC URL, so the value would inject extra connection "
                    "parameters."
                )
        parameters = {"TMODE": "ANSI", "CHARSET": "UTF8"}
        if self.teradata.database:
            # Quoted when needed: a database name may contain spaces or quotes.
            parameters["DATABASE"] = _jdbc_url_value(self.teradata.database)
        if self.teradata.port:
            parameters["DBS_PORT"] = str(self.teradata.port)
        if self.teradata.logmech:
            parameters["LOGMECH"] = _jdbc_url_value(self.teradata.logmech)
        forwarded = list(_FORWARDED_JDBC_PARAMETERS)
        if (self.teradata.logmech or "").lower() == "browser":
            forwarded += _FORWARDED_BROWSER_JDBC_PARAMETERS
        for field, jdbc_name in forwarded:
            value = getattr(self.teradata, field)
            if value is not None:
                parameters[jdbc_name] = _jdbc_url_value(value)
        ignored = [
            field
            for field in _UNFORWARDABLE_JDBC_SETTINGS
            if getattr(self.teradata, field) is not None
        ]
        if ignored:
            get_dagster_logger().warning(
                f"TeradataResource settings {ignored} have no Teradata JDBC "
                "equivalent and are not applied to the PySpark handler's JDBC "
                "reads and writes, which connect without an HTTP proxy."
            )
        parameter_str = ",".join(f"{k}={v}" for k, v in parameters.items())
        return f"jdbc:teradata://{host}/{parameter_str}"

    def _jdbc_options(self) -> dict[str, str]:
        """Connection properties shared by JDBC reads and writes.

        ``password`` is redacted by Spark's default ``spark.redaction.regex``; see
        the class docstring.
        """
        options = {
            "url": self.jdbc_url(),
            "driver": _JDBC_DRIVER,
        }
        if self.teradata.user:
            options["user"] = self.teradata.user
        if self.teradata.password:
            options["password"] = self.teradata.password
        return options

    @staticmethod
    def _spark_session() -> SparkSession:
        """Return the SparkSession to run JDBC reads through.

        ``SparkSession.getActiveSession()`` is thread-local and Dagster executes
        ops on worker threads that are usually not the thread the session was
        built on, so it returns ``None`` far more often than it should. Fall back
        to the process-wide default session (what ``SparkSession.active()`` uses
        internally; it is only available from PySpark 3.5 onwards, while this
        handler supports 3.4+). Neither classic lookup sees a Spark Connect
        session, so one is looked for explicitly and rejected with a named error.
        """
        session = SparkSession.getActiveSession()
        if session is None:
            session = getattr(SparkSession, "_instantiatedSession", None)
        if session is None:
            session = _active_spark_connect_session()
        if session is None:
            raise ValueError(
                "TeradataPySparkTypeHandler requires an active SparkSession to load "
                "inputs; create one (SparkSession.builder.getOrCreate()) before "
                "materializing or loading assets."
            )
        _reject_spark_connect(session)
        return session

    @staticmethod
    def _require_utc_jvm_timezone(spark: SparkSession) -> None:
        """Guard against silent timestamp corruption from a non-UTC JVM timezone.

        Spark's JDBC data source binds DateType/TimestampType/TimestampNTZType
        values as java.sql.Date/Timestamp objects, and those types carry no time
        zone of their own: converting between Spark's internal, zone-naive
        representation and them is done relative to ``java.util.TimeZone.getDefault()``
        rather than ``spark.sql.session.timeZone``. If the JVM's default time zone
        is not UTC, every date/time value written or read over JDBC is silently
        shifted by that zone's offset -- e.g. a JVM defaulting to UTC+5:30 writes
        02:30:00 to Teradata for a value that was 08:00:00 in the DataFrame.
        Setting ``spark.sql.session.timeZone`` does not fix this: that config only
        affects SQL date/time functions, not this conversion.

        This checks the *driver* JVM (the one this handler runs in via
        ``spark._jvm``). The JDBC reads and writes described above actually happen
        inside partition tasks, which in ``local`` mode run in the same JVM as the
        driver but on a cluster run in separate executor JVMs that the driver
        cannot query reliably (a probe job is not guaranteed to reach every
        executor, nor ones dynamic allocation adds later). Outside local mode the
        handler therefore requires the Spark configuration itself to set every
        executor's time zone -- ``-Duser.timezone=UTC`` in
        ``spark.executor.extraJavaOptions`` -- and rejects the run otherwise.
        """
        _reject_spark_connect(spark)
        default_timezone = spark._jvm.java.util.TimeZone.getDefault()  # noqa: SLF001
        utc_timezone = spark._jvm.java.util.TimeZone.getTimeZone("UTC")  # noqa: SLF001
        if not default_timezone.hasSameRules(utc_timezone):
            default_timezone_id = default_timezone.getID()
            raise ValueError(
                "TeradataPySparkTypeHandler requires the Spark driver JVM's default "
                f"time zone to be UTC to read or write DATE/TIMESTAMP columns "
                f"correctly over JDBC, but it is '{default_timezone_id}'. Setting "
                "spark.sql.session.timeZone does not fix this, because Spark's JDBC "
                "data source converts date/time values relative to "
                "java.util.TimeZone.getDefault(), not the session time zone. Set the "
                "JVM's default time zone before the driver process starts, for "
                "example with the environment variable "
                "`_JAVA_OPTIONS=-Duser.timezone=UTC`, or "
                "`JDK_JAVA_OPTIONS=-Duser.timezone=UTC` on JDK 9+. In a cluster "
                "deployment, also set the executors' default time zone to UTC, for "
                "example via the Spark configuration "
                "`spark.executor.extraJavaOptions=-Duser.timezone=UTC`."
            )
        master = spark.sparkContext.master
        if master == "local" or master.startswith("local["):
            # Tasks run in the driver JVM, which was checked above.
            return
        executor_timezone = _configured_executor_timezone(spark.sparkContext.getConf())
        # Resolved by the JVM's own rules, so e.g. "GMT" or "Etc/UTC" pass too.
        executor_rules = (
            None
            if executor_timezone is None
            else spark._jvm.java.util.TimeZone.getTimeZone(executor_timezone)  # noqa: SLF001
        )
        if executor_rules is None or not executor_rules.hasSameRules(utc_timezone):
            found = (
                "none is configured"
                if executor_timezone is None
                else f"it is configured as '{executor_timezone}'"
            )
            raise ValueError(
                "TeradataPySparkTypeHandler requires every Spark executor JVM's "
                "default time zone to be UTC to read or write DATE/TIMESTAMP columns "
                "correctly over JDBC, because the JDBC conversions run on the "
                f"executors, but {found}. The executors cannot be inspected "
                "reliably, so the Spark configuration must set it: add "
                "`-Duser.timezone=UTC` to `spark.executor.extraJavaOptions`."
            )

    _TEMPORAL_TYPES = (T.DateType, T.TimestampType, T.TimestampNTZType)

    @classmethod
    def _schema_has_temporal_columns(cls, schema: T.StructType) -> bool:
        return any(
            isinstance(field.dataType, cls._TEMPORAL_TYPES) for field in schema.fields
        )

    # JDBC options this handler always derives from the TeradataResource and the
    # asset being loaded, whether or not a given resource happens to set them. They
    # fall outside the read_partitioning allowlist anyway, but get a dedicated error
    # because supplying them is a misunderstanding of where connection settings
    # come from. "user"/"password" are listed unconditionally so read_partitioning
    # can never become a second, unaudited credentials channel, and "query" because
    # Spark rejects a read that specifies both "dbtable" and "query".
    _RESERVED_JDBC_KEYS = frozenset(
        {"url", "driver", "user", "password", "dbtable", "query"}
    )

    def _read_options(self) -> dict[str, Any]:
        """JDBC options for reads, including any partitioned-read configuration."""
        connection_options = self._jdbc_options()
        # Spark resolves JDBC option names case-insensitively, so compare casefolded:
        # {"URL": ...} would otherwise pass this check and still replace the
        # handler-selected endpoint once Spark normalizes the options.
        reserved_keys = {
            key.lower() for key in connection_options
        } | self._RESERVED_JDBC_KEYS
        reserved = sorted(
            key for key in self.read_partitioning if key.lower() in reserved_keys
        )
        if reserved:
            raise ValueError(
                f"read_partitioning must not override the reserved options "
                f"{reserved}; they are derived from the TeradataResource and the "
                "asset being loaded. Remove them from read_partitioning. Note that "
                "Spark matches option names case-insensitively."
            )
        unsupported = sorted(
            key
            for key in self.read_partitioning
            if key.lower() not in _READ_PARTITIONING_OPTIONS
        )
        if unsupported:
            raise ValueError(
                f"read_partitioning does not accept the options {unsupported}. Only "
                f"{sorted(_READ_PARTITIONING_OPTIONS.values())} are supported "
                "(matched case-insensitively, like Spark does)."
            )
        # Spark only partitions a JDBC read when all four are given, and rejects a
        # partial set with an opaque options error (numPartitions alone does not
        # partition a read at all), so validate them as a group up front.
        present = {key.lower() for key in self.read_partitioning}
        missing_group = sorted(
            _READ_PARTITIONING_OPTIONS[key]
            for key in _PARTITIONED_READ_GROUP
            if key not in present
        )
        if missing_group and len(missing_group) < len(_PARTITIONED_READ_GROUP):
            raise ValueError(
                "read_partitioning must set partitionColumn, lowerBound, upperBound "
                f"and numPartitions together; missing {missing_group}. fetchsize and "
                "queryTimeout may be set on their own."
            )
        return {**connection_options, **self.read_partitioning}

    @staticmethod
    def _validate_writable(field: T.StructField) -> None:
        """Reject Spark types that Spark itself cannot write to Teradata over JDBC.

        Spark computes a JDBC type for every field when it writes, so these fail
        inside the JVM *after* the DDL and the cleanup DELETE have been committed,
        destroying the table's previous contents. No Teradata column type can
        rescue them, which is why this runs before the ``column_types`` override.
        """
        data_type = field.dataType
        if isinstance(data_type, T.NullType):
            raise ValueError(
                f"Column '{field.name}' has type void (an all-null column with no "
                "inferred type), which Spark cannot write over JDBC. Cast it to a "
                "concrete type, for example .withColumn('"
                f"{field.name}', col('{field.name}').cast('string'))."
            )
        if isinstance(data_type, (T.ArrayType, T.MapType, T.StructType)):
            raise ValueError(
                f"Column '{field.name}' has an unsupported nested type "
                f"({data_type.simpleString()}). Flatten or serialize it (for "
                "example to a JSON string) before storing it in Teradata."
            )
        # Anything not in the supported scalar set (DayTimeIntervalType, for
        # instance) has no Teradata JDBC mapping either, so it must be rejected
        # here too rather than only where the DDL is generated -- otherwise a
        # column_types override would carry it past this point.
        writable = (T.DecimalType, T.StringType, *_SIMPLE_TYPE_MAPPING)
        if not isinstance(data_type, writable):
            raise ValueError(
                f"Column '{field.name}' has an unsupported Spark type "
                f"({data_type.simpleString()}). Cast it to a supported type before "
                "storing it in Teradata."
            )

    def column_type(self, field: T.StructField) -> str:
        """Return the Teradata column type used to create ``field``."""
        # Runs before the override: column_types picks the Teradata type, but it
        # cannot give Spark a JDBC mapping for a type Spark cannot write.
        self._validate_writable(field)
        if field.name in self.column_types:
            override = self.column_types[field.name]
            constraint = _COLUMN_CONSTRAINT.search(override)
            if constraint:
                # Only the type is validated against the frame; a constraint such
                # as NOT NULL would be violated inside the JDBC write, after the
                # cleanup DELETE had been committed and the previous rows lost.
                raise ValueError(
                    f"column_types for '{field.name}' ({override!r}) carries a "
                    f"{constraint[0].upper()!r} constraint. The PySpark handler "
                    "cannot validate constraints before it commits the cleanup, "
                    "so column_types must give the type only."
                )
            return override

        data_type = field.dataType
        if isinstance(data_type, T.DecimalType):
            if data_type.precision > _MAX_DECIMAL_PRECISION:
                raise ValueError(
                    f"Column '{field.name}' has a DecimalType with "
                    f"{data_type.precision} digits of precision, which exceeds "
                    "Teradata's maximum DECIMAL precision of "
                    f"{_MAX_DECIMAL_PRECISION}. Round the values or store the "
                    "column as a string."
                )
            return f"DECIMAL({data_type.precision},{data_type.scale})"
        if isinstance(data_type, T.StringType):
            # Explicitly UNICODE: a bare VARCHAR takes the user's default character
            # set, and a LATIN column would reject e.g. CJK values inside the JDBC
            # write, after the cleanup DELETE had been committed.
            return f"VARCHAR({self.string_length}) CHARACTER SET UNICODE"
        for spark_type, teradata_type in _SIMPLE_TYPE_MAPPING.items():
            if isinstance(data_type, spark_type):
                return teradata_type
        # Unreachable while _validate_writable and _SIMPLE_TYPE_MAPPING agree; kept
        # so the two cannot silently drift apart into returning None.
        raise ValueError(
            f"Column '{field.name}' has an unsupported Spark type "
            f"({data_type.simpleString()}). Cast it to a supported type before "
            "storing it in Teradata."
        )

    def column_types_for(self, schema: T.StructType) -> dict[str, str]:
        """Teradata type of every field, validating each one as a side effect."""
        return {field.name: self.column_type(field) for field in schema.fields}

    @staticmethod
    def _declared_char_limit(column: str, teradata_type: str) -> int | None:
        """Capacity, in characters, of a declared character type.

        Returns ``None`` when the type is not a character type or has no
        practical limit (a bare ``CLOB``). A character type whose capacity cannot
        be determined is rejected, because the check exists to fail before the
        cleanup ``DELETE`` is committed and silently skipping it would not.
        """
        parsed = parse_character_type(teradata_type)
        if parsed is None:
            return None
        if parsed.recognized:
            if parsed.base == "CLOB":
                return parsed.length
            if parsed.base == "LONG VARCHAR":
                # Byte-bounded (64000 bytes), so the character capacity depends
                # on the character set; any other case is rejected below.
                if parsed.length is None and parsed.charset in ("LATIN", "UNICODE"):
                    return 64_000 if parsed.charset == "LATIN" else 32_000
            elif parsed.length is not None:
                return parsed.length
            elif parsed.base == "CHAR":
                return 1
        raise ValueError(
            f"Cannot determine the capacity of column '{column}' from its declared "
            f"type {teradata_type!r}, so its values cannot be checked before the "
            "previous rows are deleted. Declare a sized character type such as "
            "VARCHAR(n), CHAR(n) or CLOB(n); LONG VARCHAR needs an explicit "
            "CHARACTER SET LATIN or UNICODE."
        )

    @staticmethod
    def _project_column(column: Any, is_float: bool) -> Any:
        """``column``, with NaN replaced by NULL when it is a float column."""
        if not is_float:
            return column
        return F.when(F.isnan(column), F.lit(None)).otherwise(column)

    @staticmethod
    def _max_string_lengths(
        frame: SparkDataFrame, names: Sequence[str]
    ) -> dict[str, int | None]:
        """Longest value of each named column, in UTF-16 code units (one Spark job).

        Teradata measures UNICODE character columns in UTF-16 code units, so a
        supplementary character (e.g. an emoji) takes two of a ``VARCHAR(n)``'s
        ``n``; Spark's ``length`` counts it once and would let an over-long value
        through to fail inside the JDBC write after the cleanup DELETE committed.
        For a LATIN column the count equals the character count for every value
        LATIN can store.
        """
        row = frame.agg(
            *(
                F.max(
                    (
                        F.length(F.encode(F.col(_quote_spark_name(name)), "UTF-16BE"))
                        / 2
                    ).cast("long")
                )
                for name in names
            )
        ).first()
        return {name: None if row is None else row[i] for i, name in enumerate(names)}

    def _string_limits(
        self, schema: T.StructType, column_types: Mapping[str, str]
    ) -> dict[str, int]:
        """Character capacity of every bounded StringType column, by frame name."""
        return {
            field.name: limit
            for field in schema.fields
            if isinstance(field.dataType, T.StringType)
            and (
                limit := self._declared_char_limit(field.name, column_types[field.name])
            )
            is not None
        }

    def _check_string_lengths(
        self,
        frame: SparkDataFrame,
        string_limits: Mapping[str, int],
        renames: Mapping[str, str],
    ) -> None:
        # The frame may already be renamed to the table's spelling.
        limits = {
            renames.get(name, name): limit for name, limit in string_limits.items()
        }
        if not limits:
            return
        longest = self._max_string_lengths(frame, list(limits))
        too_long = {
            name: (length, limits[name])
            for name, length in longest.items()
            if length is not None and length > limits[name]
        }
        if too_long:
            details = ", ".join(
                f"'{name}' has a {length}-character value but holds {limit}"
                for name, (length, limit) in too_long.items()
            )
            raise ValueError(
                f"Cannot store the DataFrame: {details}. Teradata would reject the "
                "insert after the previous rows had already been deleted, so nothing "
                "was written. Raise string_length, pin a wider type (or CLOB) with "
                "column_types -- migrating an existing table with ALTER TABLE -- or "
                "truncate the values."
            )

    @staticmethod
    def _latin_string_columns(
        cursor: Any,
        table_slice: TableSlice,
        schema: T.StructType,
        column_types: Mapping[str, str],
    ) -> list[str]:
        """StringType fields stored in a ``CHARACTER SET LATIN`` column.

        The catalog is authoritative (a column declared without a character set
        takes the user's default); when DBC is unreadable the declared type is
        the fallback, and a declaration without an explicit character set is
        conservatively treated as LATIN, since the default may be.
        """
        string_fields = [
            field.name
            for field in schema.fields
            if isinstance(field.dataType, T.StringType)
        ]
        if not string_fields:
            return []
        catalog_latin = latin_columns(cursor, table_slice)
        targets = []
        for name in string_fields:
            if catalog_latin is not None:
                if name.upper() in catalog_latin:
                    targets.append(name)
                continue
            parsed = parse_character_type(column_types[name])
            if parsed is not None and parsed.charset in (None, "LATIN"):
                targets.append(name)
        return targets

    @staticmethod
    def _latin_unrepresentable_counts(
        frame: SparkDataFrame, names: Sequence[str]
    ) -> dict[str, int]:
        """Values per column that Teradata LATIN cannot store (one Spark job).

        Teradata LATIN covers ISO-8859-1 plus the Windows-1252 additions, so a
        value is representable when it round-trips through either encoding.
        """

        def unrepresentable(column: Any) -> Any:
            return F.when(
                (F.decode(F.encode(column, "ISO-8859-1"), "ISO-8859-1") != column)
                & (
                    F.decode(F.encode(column, "windows-1252"), "windows-1252") != column
                ),
                1,
            )

        row = frame.agg(
            *(
                F.count(unrepresentable(F.col(_quote_spark_name(name))))
                for name in names
            )
        ).first()
        return {name: 0 if row is None else row[i] for i, name in enumerate(names)}

    def _check_latin_representable(
        self,
        frame: SparkDataFrame,
        latin_targets: Sequence[str],
        renames: Mapping[str, str],
    ) -> None:
        if not latin_targets:
            return
        names = [renames.get(name, name) for name in latin_targets]
        counts = self._latin_unrepresentable_counts(frame, names)
        bad = {name: count for name, count in counts.items() if count}
        if bad:
            details = ", ".join(
                f"'{name}' has {count} value(s)" for name, count in bad.items()
            )
            raise ValueError(
                f"Cannot store the DataFrame: {details} with characters a "
                "CHARACTER SET LATIN column cannot hold. Teradata would reject the "
                "insert after the previous rows had already been deleted, so "
                "nothing was written. Store the column as CHARACTER SET UNICODE "
                "(migrating an existing table with ALTER TABLE) or remove the "
                "characters."
            )

    @staticmethod
    def _check_for_overlapping_write(
        connection: Any, table_slice: TableSlice, row_count: int
    ) -> None:
        """Fail if the slice holds more rows than this materialization wrote.

        The cleanup DELETE is committed before the Spark write (see the class
        docstring), so nothing stops an overlapping run of the same asset from
        appending the same rows. This cannot prevent that, but it turns silent
        duplication into a failed run. Only an excess is reported: rows in the
        frame that fall outside the slice's partition predicate can make the count
        legitimately lower.
        """
        try:
            with connection.cursor() as cursor:
                cursor.execute(TeradataDbClient.get_count_statement(table_slice))
                row = cursor.fetchone()
        except teradatasql.DatabaseError:
            get_dagster_logger().warning(
                "Could not count the rows written to "
                f"{TeradataDbClient.get_quoted_table_name(table_slice)} to check for "
                "an overlapping materialization; continuing without the check.",
                exc_info=True,
            )
            return
        if row is None:
            return
        stored = int(row[0])
        if stored > row_count:
            raise RuntimeError(
                f"{TeradataDbClient.get_quoted_table_name(table_slice)} holds "
                f"{stored} rows for this materialization, but only {row_count} were "
                "written. The most likely cause is another materialization of the "
                "same asset overlapping with this one: the PySpark handler commits "
                "its cleanup DELETE before the JDBC write, so both runs appended. "
                "(A non-deterministic upstream recomputed after its cached result "
                "was evicted can cause this too.) Serialize materializations of this "
                "asset, for example with a Dagster concurrency pool, and "
                "re-materialize it to remove the duplicates."
            )

    def handle_output(
        self,
        context: OutputContext,
        table_slice: TableSlice,
        obj: SparkDataFrame,
        connection: Any,
    ) -> Mapping[str, Any]:
        _reject_spark_connect(obj)
        if not isinstance(obj, SparkDataFrame):
            raise TypeError(
                "TeradataPySparkTypeHandler can only store pyspark.sql.DataFrame "
                f"objects, got {type(obj)}."
            )
        if not obj.columns:
            raise ValueError(
                "Cannot store a DataFrame with no columns: Teradata tables require at "
                "least one column."
            )
        # Spark permits duplicate column names and Teradata identifiers are
        # case-insensitive, so labels such as "A" and "a" are exact duplicates to
        # Teradata even though Spark treats them as distinct.
        duplicate_names = case_insensitive_duplicates(obj.columns)
        if duplicate_names:
            raise ValueError(
                "Cannot store a DataFrame with duplicate column names "
                f"{duplicate_names}: Spark allows names that are exactly equal or "
                "that differ only by letter case, but Teradata identifiers are "
                "case-insensitive, so either would produce duplicate identifiers in "
                "the generated table and INSERT statements."
            )

        table_name = TeradataDbClient.get_quoted_table_name(table_slice)
        # Validate every field before touching the table. _create_table_if_absent
        # returns early when the table already exists, so without this an
        # unwritable field would only be discovered by Spark inside the JVM -- long
        # after connection.commit() below has made the cleanup DELETE permanent,
        # irreversibly losing the previous rows. Raising here instead leaves the
        # DELETE uncommitted, so it rolls back with the transaction.
        column_types = self.column_types_for(obj.schema)
        # Kept because obj is re-bound to the case-matched rename below, while
        # column_types stays keyed by the frame's own spelling.
        source_schema = obj.schema
        # Resolved now so a character type whose capacity is unknown fails before
        # any DDL runs; the values themselves are checked once the frame is cached.
        string_limits = self._string_limits(source_schema, column_types)
        # Built (and validated) now for the same reason: a missing host or a JDBC
        # separator in the resource settings must fail while the cleanup DELETE is
        # still uncommitted, not after connection.commit() below.
        jdbc_options = self._jdbc_options()

        if self._schema_has_temporal_columns(obj.schema):
            self._require_utc_jvm_timezone(obj.sparkSession)

        rename_to_match_table: dict[str, str] = {}
        with connection.cursor() as cursor:
            created = self._create_table_if_absent(
                cursor, connection, table_slice, column_types
            )
            # A table this call just created matches the frame by construction, so
            # the catalog round trip is only needed for an existing table.
            if not created:
                rename_to_match_table = validate_schema_matches_table(
                    cursor, table_slice, column_types
                )
            latin_targets = self._latin_string_columns(
                cursor, table_slice, source_schema, column_types
            )

        # delete_table_slice() has already issued the cleanup DELETE on
        # `connection`, and its WRITE lock would block any JDBC SELECT on the
        # target table. The frame's lineage may read that very table (a
        # self-dependent partitioned asset, or an in-place transformation), and
        # count() below runs it on Spark's own sessions -- which would wait for the
        # lock while this session waits for count(), hanging forever. So undo the
        # DELETE (nothing else is pending: a CREATE TABLE above was committed) and
        # re-issue it only once the frame is materialized.
        connection.rollback()

        # Spark's JDBC append resolves each DataFrame column against the fetched
        # table schema using Spark's own (possibly case-sensitive) analyzer, so a
        # column that only differs from the catalog's spelling by case -- allowed
        # above because Teradata identifiers are case-insensitive -- can still fail
        # inside Spark. Renaming to the catalog's exact spelling first avoids that
        # regardless of the spark.sql.caseSensitive setting.
        # Teradata FLOAT cannot store NaN, so it would be rejected inside the JDBC
        # write -- after the commit below made the cleanup DELETE permanent. Spark
        # keeps NaN distinct from NULL, so map it to NULL, as the pandas and polars
        # handlers do.
        float_columns = {
            field.name
            for field in source_schema.fields
            if isinstance(field.dataType, (T.FloatType, T.DoubleType))
        }
        if rename_to_match_table or float_columns:
            obj = obj.select(
                [
                    self._project_column(
                        obj[_quote_spark_name(name)], name in float_columns
                    ).alias(rename_to_match_table.get(name, name))
                    for name in obj.columns
                ]
            )

        frame = obj
        if self.write_num_partitions:
            frame = frame.repartition(self.write_num_partitions)
        # A Spark DataFrame is a lazy plan and .save() memoizes nothing, so counting
        # after the write would re-execute the whole upstream lineage a second time
        # purely for a metadata number - and, for a non-deterministic source, could
        # report a count that disagrees with what was actually written. Cache and
        # count first so the write reuses the materialized result and the number
        # describes the rows the write really saw.
        # Only evict what this handler cached. Spark's cache is keyed by logical
        # plan, so this asks the cache manager rather than trusting the object's own
        # is_cached flag: if the caller persisted the same plan -- even through a
        # different DataFrame object -- cache() is a no-op here and unpersisting
        # would evict the caller's entry too.
        caller_cached = _is_persisted(frame)
        if not caller_cached:
            frame = frame.cache()
        try:
            # count() is the first Spark action, so it is what actually evaluates the
            # upstream lineage -- with no lock held on the target table, so the
            # lineage may read it. A failure here propagates out of handle_output
            # before the cleanup DELETE has been re-issued, so the previous rows
            # survive a write that never started.
            row_count = frame.count()

            # A StringType value longer than its column's declared length would
            # be rejected by Teradata inside .save() -- after the commit below has
            # made the cleanup DELETE permanent -- so check the cached frame now,
            # while the previous rows are still intact.
            self._check_string_lengths(frame, string_limits, rename_to_match_table)
            # Likewise a value a LATIN column cannot represent (e.g. CJK text).
            self._check_latin_representable(frame, latin_targets, rename_to_match_table)

            # Re-issue the cleanup now that the frame is materialized. For an empty
            # frame it stays in the I/O manager's transaction, committed only on
            # success, and the JDBC write is skipped: there is nothing to write.
            with connection.cursor() as cursor:
                cursor.execute(TeradataDbClient.get_cleanup_statement(table_slice))
            if row_count:
                # The JDBC write below runs on Spark's own sessions, not on
                # `connection`. TeradataDbClient.connect() disables autocommit, so
                # the cleanup DELETE is still an open transaction here and holds a
                # WRITE lock on the target table. A WRITE lock is incompatible with
                # the WRITE lock the JDBC INSERTs need, so leaving it open would
                # block Spark's write until this transaction commits - which only
                # happens after handle_output() returns, i.e. never. Committing here
                # releases the lock so the write can proceed. This is deliberately
                # as late as possible - the frame is already materialized, so the
                # only remaining step is the write itself.
                # This is what makes the cleanup and the write non-atomic (see the
                # class docstring): a failed write leaves the deleted rows gone.
                # Re-materializing the asset is still correct, because the DELETE
                # simply runs again. It also means overlapping runs are not
                # serialized by this lock, which _check_for_overlapping_write()
                # detects after the fact.
                connection.commit()

                # The cleanup has already removed the rows this materialization
                # replaces, so append mode is correct here.
                (
                    frame.write.format("jdbc")
                    .mode("append")
                    .options(
                        dbtable=table_name,
                        batchsize=str(self.batch_size),
                        **jdbc_options,
                    )
                    .save()
                )
        finally:
            if not caller_cached:
                frame.unpersist()

        if row_count:
            self._check_for_overlapping_write(connection, table_slice, row_count)

        return {
            "row_count": row_count,
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
    ) -> SparkDataFrame:
        spark = self._spark_session()
        # get_select_statement applies both column selection and the partition
        # WHERE clause, so partitioned inputs only read their own rows. JDBC reads
        # take a table or subquery, so the statement is wrapped.
        select_statement = TeradataDbClient.get_select_statement(table_slice)
        # Spark matches option names case-insensitively, so "PartitionColumn" is
        # honoured by Spark and must be validated here too. Teradata identifiers are
        # likewise case-insensitive, so partitionColumn="ID" correctly selects the
        # loaded column "id" and must not be rejected.
        partition_column = next(
            (
                value
                for key, value in self.read_partitioning.items()
                if key.lower() == "partitioncolumn"
            ),
            None,
        )
        if partition_column and table_slice.columns:
            loaded = {str(column).upper() for column in table_slice.columns}
            if str(partition_column).upper() not in loaded:
                raise ValueError(
                    f"read_partitioning partitionColumn '{partition_column}' is not "
                    f"among the loaded columns {table_slice.columns}. Add it to the "
                    "input's 'columns' metadata or choose a different column."
                )
        result = (
            spark.read.format("jdbc")
            .options(
                dbtable=f"({select_statement}) AS dagster_input",
                **self._read_options(),
            )
            .load()
        )
        if self._schema_has_temporal_columns(result.schema):
            self._require_utc_jvm_timezone(spark)
        return result

    @property
    def supported_types(self) -> Sequence[type]:
        connect_frame_type = _spark_connect_dataframe_type()
        if connect_frame_type is None:
            return [SparkDataFrame]
        return [SparkDataFrame, connect_frame_type]


class TeradataPySparkIOManager(TeradataIOManager):
    """An I/O manager that stores PySpark DataFrames as Teradata tables over JDBC.

    The Teradata JDBC driver (``terajdbc4.jar``) must be on the Spark classpath,
    for example via the ``spark.jars`` Spark config.

    Example:
        .. code-block:: python

            from dagster import Definitions, EnvVar, asset
            from dagster_teradata import TeradataPySparkIOManager, TeradataResource
            from pyspark.sql import DataFrame, SparkSession

            @asset(key_prefix=["analytics"])
            def customers() -> DataFrame:
                spark = SparkSession.builder.getOrCreate()
                return spark.createDataFrame([(1, "a")], ["id", "name"])

            defs = Definitions(
                assets=[customers],
                resources={
                    "io_manager": TeradataPySparkIOManager(
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

    batch_size: int = Field(
        default=DEFAULT_BATCH_SIZE,
        ge=1,
        description="Rows the JDBC driver batches per insert round-trip.",
    )
    string_length: int = Field(
        default=DEFAULT_STRING_LENGTH,
        ge=1,
        le=_MAX_VARCHAR_LENGTH,
        description=(
            "VARCHAR width used for Spark StringType columns, whose schema carries "
            "no length. Pin wider columns via column_types."
        ),
    )
    column_types: dict[str, str] | None = Field(
        default=None,
        description=(
            "Explicit Teradata column types keyed by column name, e.g. "
            '{"amount": "DECIMAL(18,4)"}. Overrides inference for those columns.'
        ),
    )
    read_partitioning: dict[str, Any] | None = Field(
        default=None,
        description=(
            "spark.read.jdbc options for partitioned reads, e.g. "
            '{"partitionColumn": "id", "lowerBound": 0, "upperBound": 1000000, '
            '"numPartitions": 8}. Only partitionColumn, lowerBound, upperBound, '
            "numPartitions, fetchsize and queryTimeout are accepted."
        ),
    )
    write_num_partitions: int | None = Field(
        default=None,
        ge=1,
        description=(
            "Repartition the DataFrame to this many partitions before the JDBC "
            "write so that many parallel connections load the data."
        ),
    )

    def type_handlers(self) -> Sequence[DbTypeHandler]:
        return [
            TeradataPySparkTypeHandler(
                teradata=self.teradata,
                batch_size=self.batch_size,
                string_length=self.string_length,
                column_types=self.column_types,
                read_partitioning=self.read_partitioning,
                write_num_partitions=self.write_num_partitions,
            )
        ]

    @staticmethod
    def default_load_type() -> type | None:
        return SparkDataFrame


# No legacy ``@io_manager``-style definition is provided: the handler needs the
# TeradataResource at construction time to build the JDBC URL, while
# ``build_teradata_io_manager`` only resolves the resource config at run launch.
# Use TeradataPySparkIOManager instead.

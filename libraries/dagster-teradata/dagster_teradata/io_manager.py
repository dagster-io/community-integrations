"""Base I/O manager machinery for storing Dagster assets in Teradata Vantage.

``TeradataIOManager`` is an abstract :py:class:`~dagster.ConfigurableIOManagerFactory`
that wires Dagster's generic :py:class:`~dagster._core.storage.db_io_manager.DbIOManager`
to Teradata. Concrete subclasses supply the DataFrame type handlers (pandas, PySpark,
polars, ...) that perform the actual reads and writes.
"""

from abc import abstractmethod
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import datetime
from typing import Any, cast

import teradatasql
from dagster import (
    ConfigurableIOManagerFactory,
    InputContext,
    IOManagerDefinition,
    OutputContext,
    StringSource,
    TimeWindow,
    get_dagster_logger,
    io_manager,
)
from dagster import (
    Field as DagsterField,
)
from dagster._core.storage.db_io_manager import (
    DbClient,
    DbIOManager,
    DbTypeHandler,
    TablePartitionDimension,
    TableSlice,
)
from pydantic import Field

from dagster_teradata.resources import TeradataResource

TERADATA_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

# Teradata error raised when an object referenced by a statement does not exist.
_OBJECT_DOES_NOT_EXIST_ERROR = "[Error 3807]"

# Predicate that matches no rows, used when a partitioned asset is materialized or
# loaded with an empty set of partition keys.
_MATCH_NOTHING_CLAUSE = "1 = 0"


def _escape_string_literal(value: str) -> str:
    """Escape a value for safe inclusion in a single-quoted Teradata string literal."""
    return value.replace("'", "''")


def _quote_identifier(identifier: str) -> str:
    """Quote a Teradata object name so that it cannot be used for SQL injection.

    Teradata object names are case-insensitive, so quoting does not change name
    resolution; it only protects against special characters and reserved words.
    """
    return '"{}"'.format(identifier.replace('"', '""'))


def _qualified_table_name(table_slice: TableSlice) -> str:
    """Return the quoted ``database.table`` name for a table slice.

    Teradata does not have a schema namespace nested inside a database: a database
    *is* the schema. ``TableSlice.schema`` therefore holds the Teradata database name.
    """
    return f"{_quote_identifier(table_slice.schema)}.{_quote_identifier(table_slice.table)}"


def _partition_where_clause(
    partition_dimensions: Sequence[TablePartitionDimension],
) -> str:
    return " AND\n".join(
        "({})".format(
            _time_window_where_clause(partition_dimension)
            if isinstance(partition_dimension.partitions, TimeWindow)
            else _static_where_clause(partition_dimension)
        )
        for partition_dimension in partition_dimensions
    )


def _format_datetime(value: datetime) -> str:
    """Format a datetime for a Teradata TIMESTAMP(6) literal.

    ``strftime(TERADATA_DATETIME_FORMAT)`` truncates to whole seconds, which would
    silently shift partition boundaries that carry sub-second precision (for example
    custom, non-aligned time windows). Preserve microseconds when present while
    keeping the existing output for the common, second-aligned case.
    """
    formatted = value.strftime(TERADATA_DATETIME_FORMAT)
    if value.microsecond:
        formatted = f"{formatted}.{value.microsecond:06d}"
    return formatted


def _time_window_where_clause(table_partition: TablePartitionDimension) -> str:
    start_dt, end_dt = cast("TimeWindow", table_partition.partitions)
    start_dt_str = _format_datetime(start_dt)
    end_dt_str = _format_datetime(end_dt)
    # `partition_expr` is a SQL expression supplied by the asset author (for example
    # `ts` or `CAST(ts AS DATE)`), so it is interpolated as-is, matching the behaviour
    # of Dagster's first-party database I/O managers.
    column = table_partition.partition_expr
    return (
        f"{column} >= CAST('{start_dt_str}' AS TIMESTAMP(6)) AND "
        f"{column} < CAST('{end_dt_str}' AS TIMESTAMP(6))"
    )


def _static_where_clause(table_partition: TablePartitionDimension) -> str:
    partition_keys = cast("Sequence[str]", table_partition.partitions)
    if not partition_keys:
        # Dagster models "no partitions selected" as an empty partition list; emitting
        # `IN ()` would be a syntax error, so match no rows instead.
        return _MATCH_NOTHING_CLAUSE
    partitions = ", ".join(
        f"'{_escape_string_literal(partition)}'" for partition in partition_keys
    )
    return f"{table_partition.partition_expr} IN ({partitions})"


def _validate_partition_dimensions(
    context: OutputContext | InputContext, table_slice: TableSlice
) -> None:
    """Ensure every partition dimension resolved to a usable ``partition_expr``.

    For multi-partitioned assets Dagster looks the expression up per dimension in the
    ``partition_expr`` metadata mapping and can yield ``None`` when an entry is
    missing, which would otherwise be interpolated into the generated SQL.
    """
    for dimension in table_slice.partition_dimensions or []:
        if (
            not isinstance(dimension.partition_expr, str)
            or not dimension.partition_expr.strip()
        ):
            asset = context.asset_key if context.has_asset_key else table_slice.table
            raise ValueError(
                f"Asset '{asset}' is partitioned, but the 'partition_expr' metadata "
                "value does not provide a column for every partition dimension. For "
                "multi-partitioned assets set a mapping, e.g. "
                '@asset(metadata={"partition_expr": {"date": "ts", '
                '"region": "region_code"}}).'
            )


class TeradataDbClient(DbClient):
    """Executes the Teradata statements required by :py:class:`DbIOManager`.

    Unlike the ``DbClient`` implementations shipped with Dagster, this client is
    instantiated with a :py:class:`~dagster_teradata.TeradataResource` so that every
    connection detail (proxies, TLS, logmech, query band, ...) supported by the
    resource is honoured by the I/O manager as well.
    """

    def __init__(self, teradata_resource: TeradataResource):
        self.teradata_resource = teradata_resource

    @contextmanager
    def connect(
        self, context: OutputContext | InputContext, table_slice: TableSlice
    ) -> Iterator[Any]:
        # In Teradata (BTET) session mode, a DDL statement must be the last request
        # of a transaction, and a failed DDL (e.g. the expected "already exists" from
        # CREATE TABLE on the second-and-later materializations) aborts the whole
        # transaction rather than just that statement. Wrapping CREATE TABLE, DELETE,
        # and INSERT in one explicit transaction, as below, is therefore only safe in
        # ANSI mode, where each statement's outcome is independent unless explicitly
        # rolled back. Rather than special-case BTET transaction boundaries, require
        # ANSI mode for this atomicity guarantee (the rest of the client already
        # assumes ANSI mode, e.g. CASESPECIFIC string comparisons in
        # ensure_schema_exists). tmode=None is rejected too: TeradataResource then
        # omits the tmode connection parameter entirely, leaving the session mode to
        # the driver/server default, which is not guaranteed to be ANSI.
        tmode = self.teradata_resource.tmode
        if tmode is None or tmode.upper() != "ANSI":
            raise ValueError(
                f"TeradataIOManager requires the resource's tmode to be 'ANSI' to "
                f"safely wrap CREATE TABLE/DELETE/INSERT in a single transaction, but "
                f"got tmode={tmode!r}. Configure TeradataResource(tmode='ANSI') "
                "(the default) to use this I/O manager."
            )
        with self.teradata_resource.get_connection() as conn:
            try:
                # The driver auto-commits each statement by default, so without this
                # the DELETE issued by delete_table_slice() and the batched INSERTs
                # issued by a type handler's handle_output() (both run against this
                # same connection) would each commit independently: a failure partway
                # through the inserts would leave the DELETE committed and the table
                # empty or only partially rewritten. Disabling autocommit makes the
                # whole materialization one transaction, committed only on success.
                conn.autocommit = False
                try:
                    yield conn
                except BaseException:
                    conn.rollback()
                    raise
                else:
                    conn.commit()
            finally:
                # TeradataResource.get_connection() opens a new connection per call and
                # does not close it, so the session must be released here.
                conn.close()

    def ensure_schema_exists(
        self, context: OutputContext, table_slice: TableSlice, connection: Any
    ) -> None:
        """Verify the target Teradata database exists.

        Teradata databases cannot be created implicitly because they require an
        explicit ``PERM`` space allocation, so a missing database is surfaced as an
        actionable error rather than being silently created.
        """
        try:
            with connection.cursor() as cursor:
                # ``UPPER`` on both sides is required: in ANSI transaction mode string
                # comparisons are CASESPECIFIC, and DBC stores the database name with
                # the case it was created with, so a bare ``=`` would miss the row.
                cursor.execute(
                    "SELECT 1 FROM DBC.DatabasesV WHERE UPPER(DatabaseName) = UPPER(?)",
                    [table_slice.schema],
                )
                database_exists = cursor.fetchone() is not None
        except teradatasql.DatabaseError:
            # The check is advisory: users without SELECT rights on the DBC views
            # would otherwise be blocked from using the I/O manager at all. A truly
            # missing database still fails loudly when the type handler writes.
            get_dagster_logger().warning(
                f"Could not verify that Teradata database '{table_slice.schema}' exists; "
                "continuing without the check.",
                exc_info=True,
            )
            return

        if not database_exists:
            raise ValueError(
                f"Teradata database '{table_slice.schema}' does not exist. Create it "
                f"(for example: CREATE DATABASE {_quote_identifier(table_slice.schema)} "
                "AS PERM = 1000000000;) or configure the TeradataIOManager with an "
                "existing database via the 'schema' config value or asset key prefix."
            )

    def delete_table_slice(
        self, context: OutputContext, table_slice: TableSlice, connection: Any
    ) -> None:
        """Remove existing rows so that ``handle_output`` is idempotent.

        Unpartitioned assets fully replace the table, while partitioned assets only
        delete the rows belonging to the partitions being materialized.
        """
        try:
            with connection.cursor() as cursor:
                cursor.execute(self.get_cleanup_statement(table_slice))
        except teradatasql.DatabaseError as exc:
            # The table not existing yet is expected on first materialization. The
            # driver exposes no structured error code, so the message is matched.
            if _OBJECT_DOES_NOT_EXIST_ERROR not in str(exc):
                raise

    @staticmethod
    def get_cleanup_statement(table_slice: TableSlice) -> str:
        if table_slice.partition_dimensions:
            return (
                f"DELETE FROM {_qualified_table_name(table_slice)} WHERE\n"
                f"{_partition_where_clause(table_slice.partition_dimensions)}"
            )
        # An unqualified DELETE is standard SQL and valid in both ANSI and Teradata
        # session modes; the Teradata-specific `DELETE ... ALL` extension is avoided.
        return f"DELETE FROM {_qualified_table_name(table_slice)}"

    @staticmethod
    def get_select_statement(table_slice: TableSlice) -> str:
        col_str = (
            ", ".join(_quote_identifier(col) for col in table_slice.columns)
            if table_slice.columns
            else "*"
        )
        if table_slice.partition_dimensions:
            return (
                f"SELECT {col_str} FROM {_qualified_table_name(table_slice)} WHERE\n"
                f"{_partition_where_clause(table_slice.partition_dimensions)}"
            )
        return f"SELECT {col_str} FROM {_qualified_table_name(table_slice)}"

    @staticmethod
    def get_count_statement(table_slice: TableSlice) -> str:
        """``COUNT(*)`` over the rows a table slice covers.

        Read under an ACCESS lock so the count does not queue behind a concurrent
        writer's WRITE lock on the same table.
        """
        table = _qualified_table_name(table_slice)
        statement = f"LOCKING TABLE {table} FOR ACCESS SELECT COUNT(*) FROM {table}"
        if table_slice.partition_dimensions:
            statement += (
                f" WHERE\n{_partition_where_clause(table_slice.partition_dimensions)}"
            )
        return statement

    @staticmethod
    def get_table_name(table_slice: TableSlice) -> str:
        return f"{table_slice.schema}.{table_slice.table}"

    @staticmethod
    def get_quoted_table_name(table_slice: TableSlice) -> str:
        """Return the quoted ``"database"."table"`` name, for use in type handlers."""
        return _qualified_table_name(table_slice)


class TeradataDbIOManager(DbIOManager):
    """``DbIOManager`` variant adapted to Teradata's naming model.

    Teradata has no schema namespace nested inside a database, so table slices are
    always two-part ``database.table`` identifiers. The Teradata database configured
    on the ``TeradataResource`` is used as the final fallback, after Dagster's own
    resolution order (definition metadata, I/O manager config, asset key prefix).
    """

    def __init__(self, *, default_schema: str | None = None, **kwargs):
        super().__init__(**kwargs)
        self._default_schema = default_schema

    def _get_table_slice(
        self, context: OutputContext | InputContext, output_context: OutputContext
    ) -> TableSlice:
        table_slice = super()._get_table_slice(context, output_context)

        schema = table_slice.schema
        if self._uses_fallback_schema(context, output_context):
            if not self._default_schema:
                raise ValueError(
                    "Could not determine which Teradata database to use for "
                    f"'{table_slice.table}'. Set the 'schema' config value on the "
                    "TeradataIOManager, add a 'schema' metadata value or a key prefix "
                    "to the asset, or set 'database' on the TeradataResource."
                )
            schema = self._default_schema

        table_slice = table_slice._replace(database=None, schema=schema)
        _validate_partition_dimensions(context, table_slice)
        return table_slice

    def _uses_fallback_schema(
        self, context: OutputContext | InputContext, output_context: OutputContext
    ) -> bool:
        """Whether ``DbIOManager`` fell back to its generic ``"public"`` schema."""
        if (output_context.definition_metadata or {}).get("schema") or self._schema:
            return False
        return not (context.has_asset_key and len(context.asset_key.path) > 1)


class TeradataIOManager(ConfigurableIOManagerFactory):
    """Base class for I/O managers that store Dagster assets as Teradata tables.

    Subclasses implement :py:meth:`type_handlers` to declare which in-memory types
    (pandas, PySpark or polars DataFrames, ...) they can persist.

    The table an asset is stored in is derived from its asset key: the last component
    is the table name, and the Teradata database is resolved in this order of
    precedence: the ``schema`` entry of the asset's definition metadata, the ``schema``
    config value of this I/O manager, the second-to-last asset key component, and
    finally the database configured on the ``TeradataResource``. If none of these are
    set, materialization fails with a configuration error.

    Partitioned assets must declare a ``partition_expr`` metadata value naming the
    column (or SQL expression) that holds the partition value; it is interpolated into
    the generated ``WHERE`` clause verbatim, so it must not contain untrusted input.

    For ops, the database is taken from the ``schema`` entry of the output metadata and
    the table name from the ``table`` entry, falling back to the name of the output.

    .. code-block:: python

        @op(out={"my_table": Out(metadata={"schema": "my_database"})})
        def make_my_table() -> pd.DataFrame: ...

    To load only specific columns of a table into a downstream asset or op, set the
    ``columns`` metadata value on the :py:class:`~dagster.In` or
    :py:class:`~dagster.AssetIn`.

    .. code-block:: python

        @asset(ins={"my_table": AssetIn("my_table", metadata={"columns": ["a"]})})
        def my_table_a(my_table: pd.DataFrame) -> pd.DataFrame: ...

    Unlike the Snowflake and DuckDB I/O managers, a missing database is not created
    automatically: Teradata databases require an explicit ``PERM`` space allocation, so
    materialization fails with an actionable error instead.

    Example:
        .. code-block:: python

            from dagster import Definitions, asset
            from dagster_teradata import TeradataResource

            class MyTeradataIOManager(TeradataIOManager):
                def type_handlers(self):
                    return [MyDataFrameTypeHandler()]

            @asset(key_prefix=["my_database"])
            def my_table() -> pd.DataFrame: ...

            defs = Definitions(
                assets=[my_table],
                resources={
                    "io_manager": MyTeradataIOManager(
                        teradata=TeradataResource(host="...", user="...", password="...")
                    )
                },
            )
    """

    teradata: TeradataResource = Field(
        description="The TeradataResource used to connect to Teradata Vantage."
    )
    schema_: str | None = Field(
        default=None,
        alias="schema",
        description=(
            "Name of the Teradata database to store assets in. Can be overridden per "
            "asset with the 'schema' definition metadata value or an asset key prefix."
        ),
    )

    @abstractmethod
    def type_handlers(self) -> Sequence[DbTypeHandler]:
        """Type handlers that determine how to store and load the supported types."""

    @staticmethod
    def default_load_type() -> type | None:
        """Type to load inputs as when the type cannot be inferred from a type hint."""
        return None

    def create_io_manager(self, context) -> DbIOManager:
        return TeradataDbIOManager(
            db_client=TeradataDbClient(self.teradata),
            io_manager_name="TeradataIOManager",
            database="",
            schema=self.schema_,
            default_schema=self.teradata.database,
            type_handlers=self.type_handlers(),
            default_load_type=self.default_load_type(),
        )


def build_teradata_io_manager(
    type_handlers: Sequence[DbTypeHandler],
    default_load_type: type | None = None,
) -> IOManagerDefinition:
    """Build an I/O manager definition that reads and writes Teradata tables.

    Args:
        type_handlers (Sequence[DbTypeHandler]): Handlers that determine how to store
            and load the in-memory types produced and consumed by your assets and ops.
        default_load_type (Type): Type that inputs should be loaded as when the type
            cannot be inferred from a type annotation.

    Returns:
        IOManagerDefinition

    Example:
        .. code-block:: python

            teradata_io_manager = build_teradata_io_manager([MyDataFrameTypeHandler()])

            defs = Definitions(
                assets=[my_table],
                resources={
                    "io_manager": teradata_io_manager.configured(
                        {"teradata": {...}, "schema": "my_database"}
                    )
                },
            )
    """

    @io_manager(
        config_schema={
            "teradata": DagsterField(
                TeradataResource.to_config_schema().as_field().config_type,
                is_required=True,
                description="Connection configuration for the Teradata database.",
            ),
            "schema": DagsterField(
                StringSource,
                is_required=False,
                description="Name of the Teradata database to store assets in.",
            ),
        }
    )
    def teradata_io_manager(init_context):
        resource_config = dict(init_context.resource_config)
        teradata_config = dict(resource_config.get("teradata") or {})
        return TeradataDbIOManager(
            db_client=TeradataDbClient(TeradataResource(**teradata_config)),
            io_manager_name="TeradataIOManager",
            database="",
            schema=resource_config.get("schema"),
            default_schema=teradata_config.get("database"),
            type_handlers=type_handlers,
            default_load_type=default_load_type,
        )

    return teradata_io_manager

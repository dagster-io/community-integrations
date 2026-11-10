"""Database IO manager using Arrow ingestion and Dagster's table slices."""

from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Literal, Protocol, cast
from uuid import uuid4

import pyarrow as pa
from adbc_driver_manager import NotSupportedError, dbapi
from dagster import ConfigurableIOManagerFactory, InitResourceContext, InputContext, OutputContext
from dagster._core.definitions.partitions.utils import TimeWindow
from dagster._core.storage.db_io_manager import DbClient, DbIOManager, DbTypeHandler, TableSlice
from pydantic import Field

from dagster_adbc.resource import ADBCResource


@dataclass
class _Connection:
    raw: dbapi.Connection
    table_exists: bool = False
    transactional: bool = False


class ADBCClient(DbClient[_Connection]):
    """SQL and connection handling for an ADBC-backed table slice.

    Only the original ADBC ``create`` and ``append`` ingestion modes are needed.
    Table discovery uses ADBC metadata rather than catching SQL errors, which can
    invalidate a transaction on databases such as PostgreSQL.
    """

    def __init__(self, manager: "ADBCIOManager") -> None:
        self.resource = ADBCResource(
            **{field: getattr(manager, field) for field in ADBCResource.model_fields}
        )
        self.dialect = manager.dialect
        self.use_schema = manager.use_schema
        self.create_schema = manager.create_schema

    def quote(self, identifier: str) -> str:
        quote = "`" if self.dialect in ("mysql", "bigquery") else '"'
        return quote + identifier.replace(quote, quote * 2) + quote

    def table_name(self, table_slice: TableSlice) -> str:
        parts = [table_slice.table]
        if self.use_schema:
            parts.insert(0, table_slice.schema)
        if table_slice.database:
            parts.insert(0, table_slice.database)
        return ".".join(self.quote(part) for part in parts)

    def partition_clause(self, table_slice: TableSlice) -> str:
        clauses = []
        for dimension in table_slice.partition_dimensions or []:
            expr = dimension.partition_expr  # Trusted SQL expression from asset metadata.
            if isinstance(dimension.partitions, TimeWindow):
                start, end = dimension.partitions
                clauses.append(
                    f"({expr} >= '{start.isoformat(sep=' ')}' "
                    f"AND {expr} < '{end.isoformat(sep=' ')}')"
                )
            elif dimension.partitions:
                values = ", ".join(
                    "'" + key.replace("'", "''") + "'" for key in dimension.partitions
                )
                clauses.append(f"({expr} IN ({values}))")
            else:
                clauses.append("(1 = 0)")
        return " WHERE " + " AND ".join(clauses) if clauses else ""

    def get_select_statement(self, table_slice: TableSlice) -> str:
        columns = ", ".join(self.quote(column) for column in table_slice.columns or []) or "*"
        return (
            f"SELECT {columns} FROM {self.table_name(table_slice)}"
            f"{self.partition_clause(table_slice)}"
        )

    def get_table_name(self, table_slice: TableSlice) -> str:
        return self.table_name(table_slice)

    def _table_exists(self, table_slice: TableSlice, connection: dbapi.Connection) -> bool:
        with connection.adbc_get_objects(
            depth="tables",
            catalog_filter=table_slice.database or None,
            db_schema_filter=table_slice.schema if self.use_schema else None,
            table_name_filter=table_slice.table,
        ) as reader:
            for catalog in reader.read_all().to_pylist():
                if table_slice.database and catalog["catalog_name"] != table_slice.database:
                    continue
                for schema in catalog["catalog_db_schemas"] or []:
                    if self.use_schema and schema["db_schema_name"] != table_slice.schema:
                        continue
                    for table in schema["db_schema_tables"] or []:
                        if table["table_name"] == table_slice.table:
                            return True
        return False

    def delete_table_slice(
        self, context: OutputContext, table_slice: TableSlice, connection: _Connection
    ) -> None:
        # Unpartitioned outputs are replaced only after staging ingestion succeeds.
        if not table_slice.partition_dimensions:
            return
        connection.table_exists = self._table_exists(table_slice, connection.raw)
        if connection.table_exists:
            with connection.raw.cursor() as cursor:
                cursor.execute(
                    f"DELETE FROM {self.table_name(table_slice)}"
                    f"{self.partition_clause(table_slice)}"
                )

    def ensure_schema_exists(
        self, context: OutputContext, table_slice: TableSlice, connection: _Connection
    ) -> None:
        if self.create_schema and self.use_schema and self.dialect != "sqlite":
            schema = self.quote(table_slice.schema)
            if table_slice.database:
                schema = f"{self.quote(table_slice.database)}.{schema}"
            with connection.raw.cursor() as cursor:
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    @contextmanager
    def connect(
        self, context: OutputContext | InputContext, table_slice: TableSlice
    ) -> Iterator[_Connection]:
        with self.resource.get_connection() as raw:
            if self.dialect == "auto":
                try:
                    vendor = str(raw.adbc_get_info().get("vendor_name", "")).lower()
                except NotSupportedError:
                    vendor = ""
                self.dialect = next(
                    (name for name in ("sqlite", "mysql", "bigquery") if name in vendor), "ansi"
                )
            if self.use_schema is None:
                self.use_schema = self.dialect != "sqlite"
            # Some drivers silently fall back to autocommit after issuing a warning.
            # Read the effective option, rather than relying on the requested setting.
            try:
                transactional = (
                    raw.adbc_connection.get_option("adbc.connection.autocommit") == "false"
                )
            except NotSupportedError:
                # The DB-API wrapper records the effective setting, including
                # its fallback for drivers that cannot disable autocommit.
                transactional = not raw._autocommit
            try:
                yield _Connection(raw, transactional=transactional)
                if transactional:
                    raw.commit()
            except BaseException:
                if transactional:
                    raw.rollback()
                raise


class _ArrowConvertible(Protocol):
    def to_arrow(self) -> pa.Table: ...


class _ArrowTypeHandler(DbTypeHandler[object]):
    def __init__(self, client: ADBCClient, output_type: type) -> None:
        self.client = client
        self.output_type = output_type

    @property
    def supported_types(self) -> Sequence[type]:
        return [self.output_type]

    def handle_output(
        self, context: OutputContext, table_slice: TableSlice, obj: object, connection: _Connection
    ) -> Mapping[str, int]:
        if isinstance(obj, pa.Table):
            table = obj
        elif self.output_type.__module__.startswith("pandas"):
            table = pa.Table.from_pandas(obj, preserve_index=False)
        else:
            table = cast(_ArrowConvertible, obj).to_arrow()
        kwargs = {}
        if self.client.use_schema:
            kwargs["db_schema_name"] = table_slice.schema
        if table_slice.database:
            kwargs["catalog_name"] = table_slice.database
        with connection.raw.cursor() as cursor:
            if table_slice.partition_dimensions:
                cursor.adbc_ingest(
                    table_slice.table,
                    table,
                    mode="append" if connection.table_exists else "create",
                    **kwargs,
                )
            else:
                staging = table_slice._replace(table=f"__dagster_staging_{uuid4().hex}")
                try:
                    cursor.adbc_ingest(staging.table, table, mode="create", **kwargs)
                    cursor.execute(f"DROP TABLE IF EXISTS {self.client.table_name(table_slice)}")
                    cursor.execute(
                        f"ALTER TABLE {self.client.table_name(staging)} "
                        f"RENAME TO {self.client.quote(table_slice.table)}"
                    )
                except BaseException:
                    # Rollback removes transactional staging. Autocommit drivers
                    # need explicit cleanup, without masking the original failure.
                    if not connection.transactional:
                        try:
                            cursor.execute(
                                f"DROP TABLE IF EXISTS {self.client.table_name(staging)}"
                            )
                        except Exception:
                            context.log.warning(
                                "Could not remove staging table %s", self.client.table_name(staging)
                            )
                    raise
        return {"dagster/row_count": table.num_rows}

    def load_input(
        self, context: InputContext, table_slice: TableSlice, connection: _Connection
    ) -> object:
        with connection.raw.cursor() as cursor:
            cursor.execute(self.client.get_select_statement(table_slice))
            table = cursor.fetch_arrow_table()
        if self.output_type is pa.Table:
            return table
        if self.output_type.__module__.startswith("pandas"):
            return table.to_pandas()
        import polars as pl

        return pl.from_arrow(table)


class ADBCIOManager(ConfigurableIOManagerFactory):
    """Store Arrow tables and pandas/polars DataFrames through an ADBC driver.

    Install pandas or polars separately to enable their respective handlers.
    Connection configuration is identical to :class:`ADBCResource`.
    """

    driver: str | None = None
    uri: str | None = None
    profile: str | None = None
    entrypoint: str | None = None
    db_kwargs: Mapping[str, str] | None = None
    conn_kwargs: Mapping[str, str] | None = None
    autocommit: bool = False
    database: str | None = Field(default=None, description="Optional ADBC catalog name.")
    schema_: str | None = Field(default=None, alias="schema", description="Default asset schema.")
    dialect: Literal["auto", "ansi", "sqlite", "mysql", "bigquery"] = "auto"
    use_schema: bool | None = Field(
        default=None,
        description="Qualify SQL and ingestion with a schema; auto disables for SQLite.",
    )
    create_schema: bool = Field(default=True, description="Issue CREATE SCHEMA IF NOT EXISTS.")

    def create_io_manager(self, context: InitResourceContext) -> DbIOManager:
        client = ADBCClient(self)
        handlers = [_ArrowTypeHandler(client, pa.Table)]
        try:
            import pandas as pd
        except ImportError:
            pass
        else:
            handlers.append(_ArrowTypeHandler(client, pd.DataFrame))
        try:
            import polars as pl
        except ImportError:
            pass
        else:
            handlers.append(_ArrowTypeHandler(client, pl.DataFrame))
        return DbIOManager(
            type_handlers=handlers,
            db_client=client,
            database=self.database or "",
            schema=self.schema_,
            io_manager_name="ADBCIOManager",
            default_load_type=pa.Table,
        )

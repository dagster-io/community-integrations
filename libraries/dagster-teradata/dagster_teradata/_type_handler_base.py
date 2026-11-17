"""Machinery shared by the pandas, polars and PySpark type handlers.

Everything here is independent of the DataFrame library: table existence checks,
``CREATE TABLE`` handling, the Teradata error codes and limits they rely on, and
the conversions every handler needs to bind values through ``teradatasql``.
"""

from collections import Counter
from collections.abc import Iterable, Mapping
from decimal import Decimal
from typing import Any, TypeVar

import teradatasql
from dagster._core.storage.db_io_manager import DbTypeHandler, TableSlice

from dagster_teradata.io_manager import TeradataDbClient, _quote_identifier

T = TypeVar("T")

# Teradata error raised when CREATE TABLE targets a name that is already in use.
TABLE_ALREADY_EXISTS_ERROR = "[Error 3803]"

# Widest VARCHAR Teradata supports for a UNICODE column.
MAX_VARCHAR_LENGTH = 32_000

# Teradata's maximum DECIMAL precision.
MAX_DECIMAL_PRECISION = 38

# Range teradatasql can bind as an integer parameter; wider values go as Decimal.
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1


def utf16_length(value: str) -> int:
    """Width of ``value`` in UTF-16 code units, the unit of a UNICODE column.

    A supplementary character (an emoji, say) is one Python code point but two
    UTF-16 code units, so it takes two characters of a ``VARCHAR(n) CHARACTER SET
    UNICODE`` column.
    """
    return len(value.encode("utf-16-le", "surrogatepass")) // 2


def bindable_int(value: Any) -> Any:
    """Return ``value`` in a form teradatasql can bind, widening huge ints.

    teradatasql binds Python ints with ``struct.pack(">q")``, so an unsigned 64-bit
    value above 2**63-1 raises ``struct.error`` even though the handlers declare
    such columns ``DECIMAL(20,0)``. ``Decimal`` binds as a DECIMAL parameter and
    round-trips those values exactly. Anything other than an out-of-range ``int``
    is returned unchanged.
    """
    if type(value) is int and not _INT64_MIN <= value <= _INT64_MAX:
        return Decimal(value)
    return value


def case_insensitive_duplicates(names: Iterable[str]) -> list[str]:
    """Upper-cased names that occur more than once under Teradata's rules.

    Teradata identifiers are case-insensitive, so labels such as "A" and "a" are
    exact duplicates to Teradata even when the DataFrame library treats them as
    distinct.
    """
    counts = Counter(name.upper() for name in names)
    return sorted(name for name, count in counts.items() if count > 1)


_CHARACTER_TYPE_PREFIXES = ("CHAR", "VARCHAR", "CLOB", "LONG VARCHAR")


def is_string_type(teradata_type: str) -> bool:
    """Whether a declared Teradata type stores character data.

    Matches every spelling Teradata accepts for one: ``CHAR``/``CHARACTER``,
    ``VARCHAR``/``CHAR VARYING``/``CHARACTER VARYING``, ``LONG VARCHAR``, ``CLOB``
    and ``CHARACTER LARGE OBJECT``, with or without a length or character set.
    """
    base = " ".join(teradata_type.split("(", 1)[0].upper().split())
    return base.startswith(_CHARACTER_TYPE_PREFIXES)


class TeradataTableTypeHandler(DbTypeHandler[T]):
    """Base class holding the DDL and table-existence logic every handler shares.

    Subclasses decide the Teradata type of each column; this class turns those
    decisions into a table.
    """

    @staticmethod
    def _table_exists(cursor: Any, table_slice: TableSlice) -> bool | None:
        """Best-effort check for whether the target table already exists.

        Returns ``None`` (unknown) rather than raising when the caller lacks SELECT
        rights on DBC views, matching ``ensure_schema_exists``'s advisory behaviour.
        """
        try:
            # ``UPPER`` on both sides: in ANSI transaction mode string comparisons are
            # CASESPECIFIC, and DBC stores names with the case they were created with.
            cursor.execute(
                "SELECT 1 FROM DBC.TablesV WHERE UPPER(DatabaseName) = UPPER(?) "
                "AND UPPER(TableName) = UPPER(?)",
                [table_slice.schema, table_slice.table],
            )
            return cursor.fetchone() is not None
        except teradatasql.DatabaseError:
            return None

    def _create_table_if_absent(
        self,
        cursor: Any,
        connection: Any,
        table_slice: TableSlice,
        column_types: Mapping[str, str],
    ) -> bool:
        """Create the target table from ``column_types`` if it does not exist yet.

        ``column_types`` maps each column name, in order, to its Teradata type.
        Returns ``True`` only when this call created the table.

        Teradata requires a *successful* DDL statement to be the final statement of
        a transaction (a later statement in the same transaction raises "[Error
        3722] Only a COMMIT WORK or null statement is legal after a DDL Statement"
        once autocommit is disabled - see TeradataDbClient.connect()), so a
        successful CREATE TABLE is committed immediately. The transaction it ends
        only holds delete_table_slice()'s DELETE against a table that did not exist
        yet, which is a no-op.

        A CREATE TABLE that fails with "already exists" does not put the session
        into that post-DDL state, so no commit is needed on that path. It does,
        however, silently discard any DML already issued earlier in the same
        transaction (verified empirically against a live system), so
        delete_table_slice()'s DELETE is re-issued here, or the table would end up
        with both the old and the newly written rows. On every materialization
        after the first the table already exists and DDL is skipped altogether, so
        the DELETE and the write stay in one transaction.
        """
        if self._table_exists(cursor, table_slice):
            return False

        table_name = TeradataDbClient.get_quoted_table_name(table_slice)
        columns = ", ".join(
            f"{_quote_identifier(name)} {teradata_type}"
            for name, teradata_type in column_types.items()
        )
        try:
            # NO PRIMARY INDEX avoids Teradata defaulting to the first column as the
            # Primary Index, which fails with "[Error 3737] A LOB column is not
            # allowed in a Primary Index" whenever that first column is BLOB/CLOB.
            cursor.execute(f"CREATE TABLE {table_name} ({columns}) NO PRIMARY INDEX")
        except teradatasql.DatabaseError as exc:
            # Teradata has no CREATE TABLE IF NOT EXISTS. The existence check above
            # is advisory (it can return None when DBC.TablesV isn't readable), so a
            # concurrent create or an unreadable check can still land here.
            if TABLE_ALREADY_EXISTS_ERROR not in str(exc):
                raise
            cursor.execute(TeradataDbClient.get_cleanup_statement(table_slice))
            return False
        connection.commit()
        return True

    @staticmethod
    def _insert_rows(
        cursor: Any,
        table_slice: TableSlice,
        column_names: Iterable[str],
        chunks: Iterable[list[list[Any]]],
    ) -> None:
        """Insert pre-converted rows in ``executemany`` batches, one per chunk."""
        names = list(column_names)
        quoted_columns = ", ".join(_quote_identifier(name) for name in names)
        placeholders = ", ".join("?" for _ in names)
        statement = (
            f"INSERT INTO {TeradataDbClient.get_quoted_table_name(table_slice)} "
            f"({quoted_columns}) VALUES ({placeholders})"
        )
        for rows in chunks:
            cursor.executemany(statement, rows)

"""Teradata catalog reflection and schema-drift detection shared by type handlers.

Nothing here is specific to a DataFrame library: callers pass the frame's column
names alongside the Teradata types they would declare for them, and get back the
existing table's exact column spellings, or a ``ValueError`` describing the drift.
"""

import re
from collections.abc import Mapping, Sequence
from typing import Any, NamedTuple

import teradatasql
from dagster._core.storage.db_io_manager import TableSlice

from dagster_teradata.io_manager import TeradataDbClient

# DBC.ColumnsV ColumnType codes, verified against a live Teradata system rather
# than taken from the manual: BYTEINT is "I1" (not "BY", which is fixed-width
# BYTE) and TIMESTAMP WITH TIME ZONE is "SZ" (not "TZ", which is TIME WITH TIME
# ZONE). Only codes the type handlers can also *declare* are listed; anything else
# in an existing table is left uncompared rather than guessed at.
_UNPARAMETERIZED_DBC_CODES = {
    "I1": "BYTEINT",
    "I2": "SMALLINT",
    "I": "INTEGER",
    "I8": "BIGINT",
    "F": "FLOAT",
    "DA": "DATE",
}

# DBC CharType values whose ColumnLength -> character count conversion is known:
# LATIN stores one byte per character and UNICODE two. GRAPHIC/KANJI types also
# store multiple bytes per character, but not at a ratio this module models.
_BYTES_PER_CHAR = {1: 1, 2: 2}

# Spellings Teradata accepts for the same type, so a column_types override written
# as "INT" or "NUMERIC(10,2)" is not mistaken for drift.
_TYPE_ALIASES = {
    "INT": "INTEGER",
    "NUMERIC": "DECIMAL",
    "DEC": "DECIMAL",
    "REAL": "FLOAT",
    "DOUBLE PRECISION": "FLOAT",
}

# BASE, BASE(n) or BASE(p,s), optionally followed by WITH TIME ZONE. LOB sizes may
# carry a K/M/G multiplier, e.g. CLOB(100K). Character types are handled first by
# parse_character_type; any other elaborate declaration deliberately fails to
# match, so the type comparison skips it instead of reporting a false mismatch.
_TYPE_PATTERN = re.compile(
    r"^(?P<base>[A-Z]+(?: PRECISION)?)"
    r"(?:\((?P<first>\d+)(?P<unit>[KMG])?(?:,(?P<second>\d+))?\))?"
    r"(?P<tz> WITH TIME ZONE)?$"
)

_LOB_TYPES = {"BLOB", "CLOB"}
_LOB_SIZE_UNITS = {None: 1, "K": 1024, "M": 1024**2, "G": 1024**3}

# A declared Teradata character type in any spelling Teradata accepts, followed by
# whatever else the declaration carries. Longer spellings come first so "CHAR"
# cannot shadow them.
_CHARACTER_TYPE = re.compile(
    r"(?P<base>CHARACTER\s+LARGE\s+OBJECT|CHARACTER\s+VARYING|CHAR\s+VARYING"
    r"|LONG\s+VARCHAR|VARCHAR|CLOB|CHARACTER|CHAR)(?![A-Z0-9_$#])"
    r"\s*(?:\(\s*(?P<length>\d+)\s*(?P<unit>[KMG])?\s*\))?(?P<rest>.*)",
    re.IGNORECASE | re.DOTALL,
)
_CHARACTER_BASES = {
    "CHARACTER LARGE OBJECT": "CLOB",
    "CHARACTER VARYING": "VARCHAR",
    "CHAR VARYING": "VARCHAR",
    "CHARACTER": "CHAR",
}
_CHARACTER_SET = re.compile(r"\bCHARACTER\s+SET\s+(?P<name>\w+)", re.IGNORECASE)
# Attributes that do not change a column's type or capacity.
_HARMLESS_ATTRIBUTE = re.compile(
    r"\b(?:NOT\s+)?(?:CASESPECIFIC|CS|NULL)\b", re.IGNORECASE
)


class CharacterType(NamedTuple):
    """A parsed character declaration.

    ``base`` is CHAR, VARCHAR, LONG VARCHAR or CLOB; ``length`` is the declared
    capacity in characters (LOB units applied), ``None`` when omitted; ``charset``
    is the upper-cased CHARACTER SET, ``None`` when omitted. ``recognized`` is
    ``False`` when the declaration carries something this parser does not model
    (an unknown attribute, or a K/M/G unit on a non-LOB), so callers must not rely
    on the other fields.
    """

    base: str
    length: int | None
    charset: str | None
    recognized: bool


def parse_character_type(declared: str) -> CharacterType | None:
    """Parse a character declaration; ``None`` if it is not a character type."""
    match = _CHARACTER_TYPE.fullmatch(declared.strip())
    if not match:
        return None
    base = " ".join(match["base"].upper().split())
    base = _CHARACTER_BASES.get(base, base)
    unit = match["unit"] and match["unit"].upper()
    length = match["length"]
    rest = match["rest"]
    charset = None
    if charset_match := _CHARACTER_SET.search(rest):
        charset = charset_match["name"].upper()
        rest = rest[: charset_match.start()] + rest[charset_match.end() :]
    recognized = not _HARMLESS_ATTRIBUTE.sub("", rest).strip()
    if unit and base != "CLOB":
        recognized = False
    return CharacterType(
        base,
        None if length is None else int(length) * _LOB_SIZE_UNITS[unit],
        charset,
        recognized,
    )


def _canonical_character_type(parsed: CharacterType) -> str | None:
    """Catalog-comparable form of a parsed character type, else ``None``."""
    if not parsed.recognized or parsed.charset not in (None, "LATIN", "UNICODE"):
        return None
    if parsed.base == "CLOB":
        if parsed.length is None:
            return "CLOB"
        if parsed.length in _MAX_LOB_LENGTHS["CLOB"]:
            return "CLOB"
        return f"CLOB({parsed.length})"
    if parsed.base == "LONG VARCHAR":
        # A byte-bounded alias of VARCHAR(64000 bytes); the character count
        # depends on the character set, which must therefore be explicit.
        if parsed.length is not None or parsed.charset is None:
            return None
        return f"VARCHAR({64_000 if parsed.charset == 'LATIN' else 32_000})"
    if parsed.base == "CHAR" and parsed.length is None:
        return "CHAR(1)"
    if parsed.length is None:
        return None
    return f"{parsed.base}({parsed.length})"


# DBC.ColumnsV ColumnLength of a bare BLOB/CLOB, verified live: Teradata's maximum
# LOB size in bytes, whatever the character set. A bare UNICODE CLOB therefore
# holds half as many characters as a bare LATIN one.
_MAX_LOB_BYTES = 2_097_088_000
# Character counts that equal a bare LOB in some character set, so an explicit
# declaration of one is the same column as a bare CLOB/BLOB.
_MAX_LOB_LENGTHS = {
    "BLOB": {_MAX_LOB_BYTES},
    "CLOB": {_MAX_LOB_BYTES // n for n in (1, 2)},
}

# DBC.TablesV TableKind values for base tables: "T" is a table with a primary
# index and "O" one created NO PRIMARY INDEX, which is how the handlers create it.
_BASE_TABLE_KINDS = ("T", "O")


def canonical_teradata_type(declared: str) -> str | None:
    """Normalize a Teradata type so declared and catalog forms compare literally.

    Returns ``None`` for anything this module cannot confidently parse -- an
    exotic ``column_types`` override, for instance -- so the caller skips the
    comparison rather than rejecting a column it simply does not understand.
    """
    text = " ".join(declared.upper().split()).replace(", ", ",")
    parsed = parse_character_type(text)
    if parsed:
        return _canonical_character_type(parsed)
    match = _TYPE_PATTERN.match(text)
    if not match:
        return None
    base = _TYPE_ALIASES.get(match["base"], match["base"])
    first, second, tz = match["first"], match["second"], match["tz"]
    if base in _LOB_TYPES:
        # A bare LOB is Teradata's maximum size; a sized one must be compared,
        # since a wider declaration than the existing column is real drift.
        if second is not None or tz:
            return None
        if first is None:
            return base
        length = int(first) * _LOB_SIZE_UNITS[match["unit"]]
        return base if length in _MAX_LOB_LENGTHS[base] else f"{base}({length})"
    if match["unit"]:
        return None
    if base in ("VARCHAR", "CHAR"):
        return f"{base}({first})" if first is not None else None
    if base == "DECIMAL":
        # A bare DECIMAL means Teradata's own default precision; refuse to guess.
        return None if first is None or second is None else f"DECIMAL({first},{second})"
    if base in ("TIMESTAMP", "TIME"):
        if first is None:
            return None
        suffix = " WITH TIME ZONE" if tz else ""
        return f"{base}({first}){suffix}"
    if first is not None or tz:
        return None
    return base if base in _UNPARAMETERIZED_DBC_CODES.values() else None


def catalog_type(row: Sequence[Any]) -> str | None:
    """Rebuild a canonical type string from DBC.ColumnsV attributes."""
    code, length, total_digits, fractional_digits, char_type = row
    if code == "CV" or code == "CF":
        # ColumnLength is a byte count, so convert back to the declared character
        # length. An unrecognized CharType would yield a wrong count -- and so a
        # false drift error -- so it is skipped instead, like any other type this
        # module cannot model.
        bytes_per_char = _BYTES_PER_CHAR.get(char_type)
        if bytes_per_char is None:
            return None
        chars = length // bytes_per_char
        return f"{'VARCHAR' if code == 'CV' else 'CHAR'}({chars})"
    if code == "D":
        return f"DECIMAL({total_digits},{fractional_digits})"
    if code in ("BO", "CO"):
        base = "BLOB" if code == "BO" else "CLOB"
        if length == _MAX_LOB_BYTES:
            return base
        if code == "BO":
            return f"BLOB({length})"
        bytes_per_char = _BYTES_PER_CHAR.get(char_type)
        if bytes_per_char is None:
            return None
        chars = length // bytes_per_char
        # A sized LATIN CLOB whose length happens to equal a bare UNICODE CLOB's
        # would canonicalize to the same text as a bare declaration, so leave it
        # uncompared rather than risk a false mismatch.
        return None if chars in _MAX_LOB_LENGTHS["CLOB"] else f"CLOB({chars})"
    if code == "TS":
        return f"TIMESTAMP({fractional_digits})"
    if code == "SZ":
        return f"TIMESTAMP({fractional_digits}) WITH TIME ZONE"
    if code == "AT":
        return f"TIME({fractional_digits})"
    if code == "TZ":
        return f"TIME({fractional_digits}) WITH TIME ZONE"
    return _UNPARAMETERIZED_DBC_CODES.get(code)


class ExistingColumn(NamedTuple):
    """A target table's column as reported by DBC.ColumnsV.

    ``name`` preserves the table's exact spelling (Teradata identifiers are
    case-insensitive, but e.g. Spark's JDBC append is not) and ``type`` is the
    canonical Teradata type string, or ``None`` if this module cannot express it.
    """

    name: str
    type: str | None


def existing_columns(
    cursor: Any, table_slice: TableSlice
) -> dict[str, ExistingColumn] | None:
    """Map the target table's upper-cased column names to their spelling and type.

    A ``type`` of ``None`` means the column's type is one the handlers cannot
    declare, so it is left uncompared. The lookup is advisory: it returns ``None``
    rather than raising when DBC is unreadable or the target does not exist. A
    target that exists but is not a base table -- a view, say, which
    ``delete_table_slice()`` may already have deleted through -- raises instead,
    because it can neither be validated nor safely appended to.
    """
    try:
        # ``UPPER`` on both sides: in ANSI transaction mode string comparisons are
        # CASESPECIFIC, and DBC stores names with the case they were created with.
        cursor.execute(
            "SELECT c.ColumnName, c.ColumnType, c.ColumnLength, "
            "c.DecimalTotalDigits, c.DecimalFractionalDigits, c.CharType, "
            "t.TableKind "
            "FROM DBC.ColumnsV c JOIN DBC.TablesV t "
            "ON c.DatabaseName = t.DatabaseName AND c.TableName = t.TableName "
            "WHERE UPPER(c.DatabaseName) = UPPER(?) AND UPPER(c.TableName) = UPPER(?)",
            [table_slice.schema, table_slice.table],
        )
        rows = list(cursor.fetchall())
    except teradatasql.DatabaseError:
        return None
    if not rows:
        # No rows means the table does not exist (or DBC is not readable);
        # either way there is nothing to compare against.
        return None
    # Tables and views share one namespace per database, so these rows all
    # describe the target object itself.
    kind = str(rows[0][6]).strip()
    if kind not in _BASE_TABLE_KINDS:
        raise ValueError(
            f"{TeradataDbClient.get_quoted_table_name(table_slice)} exists but is "
            f"not a base table (DBC TableKind '{kind}', e.g. 'V' for a view). The "
            "I/O manager only writes to tables it can create and validate; point "
            "the asset at a table, or drop the object so the table is created."
        )
    # DBC pads names on the right; leading blanks are part of a quoted identifier
    # (" id" is not "id"), so only the right side is stripped.
    return {
        row[0].rstrip().upper(): ExistingColumn(
            name=row[0].rstrip(),
            type=catalog_type((row[1].strip(), row[2], row[3], row[4], row[5])),
        )
        for row in rows
    }


def zoned_timestamp_columns(cursor: Any, table_slice: TableSlice) -> set[str] | None:
    """Upper-cased names of the table's ``TIMESTAMP WITH TIME ZONE`` columns.

    ``cursor.description`` reports both zoned and naive timestamps as
    ``datetime.datetime``, so a column with no values to infer from needs the
    catalog to tell them apart. Returns ``None`` if DBC is not readable. Views
    are not described in ``DBC.ColumnsV`` with types, so they yield no names.
    """
    try:
        cursor.execute(
            "SELECT ColumnName FROM DBC.ColumnsV "
            "WHERE UPPER(DatabaseName) = UPPER(?) AND UPPER(TableName) = UPPER(?) "
            "AND ColumnType = 'SZ'",
            [table_slice.schema, table_slice.table],
        )
        rows = list(cursor.fetchall())
    except teradatasql.DatabaseError:
        return None
    return {row[0].rstrip().upper() for row in rows}


def latin_columns(cursor: Any, table_slice: TableSlice) -> set[str] | None:
    """Upper-cased names of the table's ``CHARACTER SET LATIN`` character columns.

    A string column declared without a character set takes the creating user's
    default, which may be LATIN, so only the catalog says how it was created.
    Returns ``None`` if DBC is not readable.
    """
    try:
        cursor.execute(
            "SELECT ColumnName FROM DBC.ColumnsV "
            "WHERE UPPER(DatabaseName) = UPPER(?) AND UPPER(TableName) = UPPER(?) "
            "AND ColumnType IN ('CV', 'CF', 'CO') AND CharType = 1",
            [table_slice.schema, table_slice.table],
        )
        rows = list(cursor.fetchall())
    except teradatasql.DatabaseError:
        return None
    return {row[0].rstrip().upper() for row in rows}


def validate_schema_matches_table(
    cursor: Any, table_slice: TableSlice, declared_types: Mapping[str, str]
) -> dict[str, str]:
    """Fail if a frame has drifted from the existing table.

    ``declared_types`` maps each frame column name, in order, to the Teradata type
    the handler would declare for it. Tables are created once and then reused, so a
    renamed, added or dropped column -- or a column whose type changed, such as an
    ``id INTEGER`` column rematerialized as a string -- leaves the frame and the
    table disagreeing. Catching that before any irreversible step lets the cleanup
    DELETE roll back with the transaction.

    Returns a mapping of frame column name -> the table's exact spelling for
    columns that differ only by case, so a caller whose writer resolves columns
    case-sensitively can rename them first.

    Only call this for a table known to exist. If its columns cannot be read from
    DBC, this raises rather than treating the unvalidated schema as a match,
    since a mismatch would only surface after the cleanup DELETE was committed.
    """
    existing = existing_columns(cursor, table_slice)
    table_name = TeradataDbClient.get_quoted_table_name(table_slice)
    if existing is None:
        raise ValueError(
            f"Cannot read the column definitions of the existing table "
            f"{table_name} from DBC.ColumnsV/DBC.TablesV, so its schema cannot be "
            "validated against the DataFrame before the previous rows are deleted. "
            "Grant the user SELECT on DBC.ColumnsV and DBC.TablesV (and access to "
            "the table), or drop the table so it is recreated from the DataFrame."
        )
    # Teradata identifiers are case-insensitive; callers have already rejected
    # frames whose names collide under that rule.
    frame_names = {name.upper() for name in declared_types}
    missing = sorted(frame_names - set(existing))
    unexpected = sorted(set(existing) - frame_names)
    if missing or unexpected:
        details = []
        if missing:
            details.append(f"columns not present in the table: {missing}")
        if unexpected:
            details.append(f"table columns not present in the DataFrame: {unexpected}")
        raise ValueError(
            f"The DataFrame's schema does not match the existing table "
            f"{table_name} ({'; '.join(details)}). This handler creates the "
            "table once and appends to it afterwards, so it cannot add, drop "
            "or rename columns. Migrate the table with ALTER TABLE, or drop it "
            "so it is recreated from the new schema."
        )

    rename_to_match_table: dict[str, str] = {}
    for name, declared_type in declared_types.items():
        actual = existing[name.upper()]
        if actual.name != name:
            rename_to_match_table[name] = actual.name
        declared = canonical_teradata_type(declared_type)
        # Skip rather than guess when either side is a form this module does not
        # model (an exotic column_types override, or a column created by hand as a
        # type the handlers never emit): a false mismatch would block a write
        # Teradata would have accepted.
        if actual.type is None or declared is None or actual.type == declared:
            continue
        raise ValueError(
            f"Column '{name}' is {actual.type} in the existing table "
            f"{table_name}, but this materialization would store it as "
            f"{declared}. Column types must match the existing table exactly "
            "(even widening conversions are rejected, since a mismatched append "
            "can fail or silently corrupt the column), and this is checked before "
            "the previous rows are deleted. "
            "Migrate the column with ALTER TABLE, pin the type with column_types, "
            "or drop the table so it is recreated."
        )
    return rename_to_match_table

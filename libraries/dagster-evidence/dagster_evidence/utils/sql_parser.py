"""SQL parsing utilities for extracting table references.

This module provides utilities for parsing SQL queries and extracting
table references that can be used to determine asset dependencies.
"""

import sqlglot
from sqlglot import exp

from dagster._annotations import beta, public


@beta
@public
def extract_table_references(
    sql_query: str,
    default_database: str | None = None,
    default_schema: str | None = None,
) -> list[dict[str, str | None]]:
    """Parse SQL and extract table references.

    Uses SQLGlot to parse SQL queries and extract all table references,
    including tables in JOINs, subqueries, and CTEs.

    Args:
        sql_query: The SQL query to parse.
        default_database: Default database name to use if not specified in query.
        default_schema: Default schema name to use if not specified in query.

    Returns:
        List of dictionaries, each containing:
        - database: The database/catalog name (or default_database if not specified)
        - schema: The schema name (or default_schema if not specified)
        - table: The table name

    Example:

        .. code-block:: python

            from dagster_evidence.utils import extract_table_references

            # Simple query
            refs = extract_table_references("SELECT * FROM orders")
            # Returns: [{"database": None, "schema": None, "table": "orders"}]

            # With defaults
            refs = extract_table_references(
                "SELECT * FROM orders",
                default_database="my_db",
                default_schema="main"
            )
            # Returns: [{"database": "my_db", "schema": "main", "table": "orders"}]

            # Fully qualified table
            refs = extract_table_references("SELECT * FROM prod.sales.orders")
            # Returns: [{"database": "prod", "schema": "sales", "table": "orders"}]
    """
    try:
        parsed = sqlglot.parse(sql_query)
        tables: list[dict[str, str | None]] = []

        for statement in parsed:
            if statement is None:
                continue
            for table in statement.find_all(exp.Table):
                tables.append(
                    {
                        "database": table.catalog or default_database,
                        "schema": table.db or default_schema,
                        "table": table.name,
                    }
                )

        return tables
    except Exception:
        return []

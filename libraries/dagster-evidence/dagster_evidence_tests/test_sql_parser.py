"""Tests for SQL parser utilities."""

from dagster_evidence.utils import extract_table_references


class TestExtractTableReferences:
    """Tests for extract_table_references function."""

    def test_simple_select(self):
        """Test parsing a simple SELECT statement."""
        refs = extract_table_references("SELECT * FROM orders")
        assert len(refs) == 1
        assert refs[0]["table"] == "orders"
        assert refs[0]["schema"] is None
        assert refs[0]["database"] is None

    def test_select_with_schema(self):
        """Test parsing SELECT with schema-qualified table name."""
        refs = extract_table_references("SELECT * FROM sales.orders")
        assert len(refs) == 1
        assert refs[0]["table"] == "orders"
        assert refs[0]["schema"] == "sales"
        assert refs[0]["database"] is None

    def test_fully_qualified_table(self):
        """Test parsing fully qualified database.schema.table reference."""
        refs = extract_table_references("SELECT * FROM prod.sales.orders")
        assert len(refs) == 1
        assert refs[0]["table"] == "orders"
        assert refs[0]["schema"] == "sales"
        assert refs[0]["database"] == "prod"

    def test_inner_join(self):
        """Test parsing query with INNER JOIN."""
        refs = extract_table_references("""
            SELECT o.*, c.name
            FROM orders o
            INNER JOIN customers c ON o.customer_id = c.id
        """)
        assert len(refs) == 2
        tables = {ref["table"] for ref in refs}
        assert tables == {"orders", "customers"}

    def test_left_join(self):
        """Test parsing query with LEFT JOIN."""
        refs = extract_table_references("""
            SELECT o.*, p.name
            FROM orders o
            LEFT JOIN products p ON o.product_id = p.id
        """)
        assert len(refs) == 2
        tables = {ref["table"] for ref in refs}
        assert tables == {"orders", "products"}

    def test_multiple_joins(self):
        """Test parsing query with multiple JOINs."""
        refs = extract_table_references("""
            SELECT o.*, c.name, p.name
            FROM orders o
            JOIN customers c ON o.customer_id = c.id
            LEFT JOIN products p ON o.product_id = p.id
            RIGHT JOIN shipments s ON o.shipment_id = s.id
        """)
        assert len(refs) == 4
        tables = {ref["table"] for ref in refs}
        assert tables == {"orders", "customers", "products", "shipments"}

    def test_cte_query(self):
        """Test parsing query with Common Table Expression (CTE)."""
        refs = extract_table_references("""
            WITH daily_sales AS (
                SELECT date, SUM(amount) as total
                FROM orders
                GROUP BY date
            )
            SELECT * FROM daily_sales
            JOIN targets ON daily_sales.date = targets.date
        """)
        # Should include orders, daily_sales (CTE reference), and targets
        tables = {ref["table"] for ref in refs}
        assert "orders" in tables
        assert "targets" in tables
        # CTEs are also captured as table references
        assert "daily_sales" in tables

    def test_subquery(self):
        """Test parsing query with subquery."""
        refs = extract_table_references("""
            SELECT *
            FROM orders
            WHERE customer_id IN (
                SELECT id FROM customers WHERE status = 'active'
            )
        """)
        tables = {ref["table"] for ref in refs}
        assert tables == {"orders", "customers"}

    def test_default_database_applied(self):
        """Test that default_database is applied when not specified in query."""
        refs = extract_table_references(
            "SELECT * FROM orders", default_database="my_db"
        )
        assert len(refs) == 1
        assert refs[0]["table"] == "orders"
        assert refs[0]["database"] == "my_db"

    def test_default_schema_applied(self):
        """Test that default_schema is applied when not specified in query."""
        refs = extract_table_references("SELECT * FROM orders", default_schema="main")
        assert len(refs) == 1
        assert refs[0]["table"] == "orders"
        assert refs[0]["schema"] == "main"

    def test_default_database_and_schema_applied(self):
        """Test that both defaults are applied when not specified in query."""
        refs = extract_table_references(
            "SELECT * FROM orders", default_database="my_db", default_schema="public"
        )
        assert len(refs) == 1
        assert refs[0]["table"] == "orders"
        assert refs[0]["database"] == "my_db"
        assert refs[0]["schema"] == "public"

    def test_defaults_not_override_explicit(self):
        """Test that defaults don't override explicit schema/database in query."""
        refs = extract_table_references(
            "SELECT * FROM explicit_db.explicit_schema.orders",
            default_database="default_db",
            default_schema="default_schema",
        )
        assert len(refs) == 1
        assert refs[0]["table"] == "orders"
        assert refs[0]["database"] == "explicit_db"
        assert refs[0]["schema"] == "explicit_schema"

    def test_invalid_sql_returns_empty(self):
        """Test that invalid SQL gracefully returns empty list."""
        refs = extract_table_references("THIS IS NOT VALID SQL !!!")
        assert refs == []

    def test_empty_sql_returns_empty(self):
        """Test that empty SQL returns empty list."""
        refs = extract_table_references("")
        assert refs == []

    def test_multiple_tables_same_query(self):
        """Test parsing query referencing same table multiple times."""
        refs = extract_table_references("""
            SELECT a.*, b.*
            FROM orders a
            JOIN orders b ON a.parent_id = b.id
        """)
        # Both references to 'orders' should be captured
        assert len(refs) == 2
        assert all(ref["table"] == "orders" for ref in refs)

    def test_union_query(self):
        """Test parsing UNION query with multiple tables."""
        refs = extract_table_references("""
            SELECT id, name FROM customers
            UNION
            SELECT id, name FROM suppliers
        """)
        tables = {ref["table"] for ref in refs}
        assert tables == {"customers", "suppliers"}

    def test_insert_statement(self):
        """Test parsing INSERT statement."""
        refs = extract_table_references("""
            INSERT INTO target_table
            SELECT * FROM source_table
        """)
        tables = {ref["table"] for ref in refs}
        assert "target_table" in tables
        assert "source_table" in tables

    def test_complex_query_with_mixed_qualifications(self):
        """Test complex query with mixed table qualifications."""
        refs = extract_table_references(
            """
            SELECT a.*, b.*, c.*
            FROM unqualified_table a
            JOIN schema1.schema_qualified b ON a.id = b.id
            JOIN db.schema2.fully_qualified c ON b.id = c.id
            """,
            default_database="default_db",
            default_schema="default_schema",
        )
        assert len(refs) == 3

        # Find each reference
        unqualified = next(r for r in refs if r["table"] == "unqualified_table")
        schema_qualified = next(r for r in refs if r["table"] == "schema_qualified")
        fully_qualified = next(r for r in refs if r["table"] == "fully_qualified")

        # Unqualified should get defaults
        assert unqualified["database"] == "default_db"
        assert unqualified["schema"] == "default_schema"

        # Schema-qualified should get default database but explicit schema
        assert schema_qualified["database"] == "default_db"
        assert schema_qualified["schema"] == "schema1"

        # Fully qualified should keep all explicit values
        assert fully_qualified["database"] == "db"
        assert fully_qualified["schema"] == "schema2"

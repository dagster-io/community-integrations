"""Tests for Evidence project source classes."""

from dagster_evidence.components.sources import (
    BigQueryEvidenceProjectSource,
    DuckdbEvidenceProjectSource,
    MotherDuckEvidenceProjectSource,
    SourceConnection,
    SourceContent,
    SourceQuery,
)

# Sample data constants (duplicated from conftest for direct import)
SAMPLE_DUCKDB_CONNECTION = {
    "name": "needful_things",
    "type": "duckdb",
    "options": {"filename": "data.duckdb"},
}

SAMPLE_MOTHERDUCK_CONNECTION = {
    "name": "analytics",
    "type": "motherduck",
    "options": {"token": "md_test_token"},
}

SAMPLE_BIGQUERY_CONNECTION = {
    "name": "warehouse",
    "type": "bigquery",
    "options": {"project": "my-project", "dataset": "analytics"},
}

SAMPLE_QUERIES = [
    {"name": "orders", "content": "SELECT * FROM orders"},
    {"name": "customers", "content": "SELECT * FROM customers"},
]


class TestSourceTypes:
    """Tests for source type identification."""

    def test_duckdb_source_type(self):
        """Verify DuckdbEvidenceProjectSource returns correct type."""
        assert DuckdbEvidenceProjectSource.get_source_type() == "duckdb"

    def test_motherduck_source_type(self):
        """Verify MotherDuckEvidenceProjectSource returns correct type."""
        assert MotherDuckEvidenceProjectSource.get_source_type() == "motherduck"

    def test_bigquery_source_type(self):
        """Verify BigQueryEvidenceProjectSource returns correct type."""
        assert BigQueryEvidenceProjectSource.get_source_type() == "bigquery"


class TestSourceContent:
    """Tests for SourceContent data class."""

    def test_source_content_from_dict(self):
        """Verify SourceContent.from_dict parses correctly."""
        source_content = SourceContent.from_dict(
            {
                "connection": SAMPLE_DUCKDB_CONNECTION,
                "queries": SAMPLE_QUERIES,
            }
        )
        assert source_content.connection.type == "duckdb"
        assert len(source_content.queries) == 2
        assert source_content.queries[0].name == "orders"
        assert source_content.queries[1].name == "customers"

    def test_source_content_connection_extra(self):
        """Verify connection extra fields are captured."""
        source_content = SourceContent.from_dict(
            {
                "connection": SAMPLE_DUCKDB_CONNECTION,
                "queries": [],
            }
        )
        assert source_content.connection.extra.get("options") == {
            "filename": "data.duckdb"
        }
        assert source_content.connection.extra.get("name") == "needful_things"

    def test_source_content_empty_queries(self):
        """Verify SourceContent handles empty queries."""
        source_content = SourceContent.from_dict(
            {
                "connection": SAMPLE_DUCKDB_CONNECTION,
                "queries": [],
            }
        )
        assert len(source_content.queries) == 0


class TestSourceClasses:
    """Tests for source class instantiation."""

    def test_duckdb_source_instantiation(self):
        """Verify DuckdbEvidenceProjectSource can be instantiated."""
        source_content = SourceContent.from_dict(
            {
                "connection": SAMPLE_DUCKDB_CONNECTION,
                "queries": SAMPLE_QUERIES,
            }
        )
        source = DuckdbEvidenceProjectSource(source_content)
        assert source.source_content.connection.type == "duckdb"

    def test_motherduck_source_instantiation(self):
        """Verify MotherDuckEvidenceProjectSource can be instantiated."""
        source_content = SourceContent.from_dict(
            {
                "connection": SAMPLE_MOTHERDUCK_CONNECTION,
                "queries": [{"name": "events", "content": "SELECT * FROM events"}],
            }
        )
        source = MotherDuckEvidenceProjectSource(source_content)
        assert source.source_content.connection.type == "motherduck"

    def test_bigquery_source_instantiation(self):
        """Verify BigQueryEvidenceProjectSource can be instantiated."""
        source_content = SourceContent.from_dict(
            {
                "connection": SAMPLE_BIGQUERY_CONNECTION,
                "queries": [
                    {"name": "transactions", "content": "SELECT * FROM transactions"}
                ],
            }
        )
        source = BigQueryEvidenceProjectSource(source_content)
        assert source.source_content.connection.type == "bigquery"


class TestSourceQuery:
    """Tests for SourceQuery data class."""

    def test_source_query_creation(self):
        """Verify SourceQuery can be created."""
        query = SourceQuery(name="test_query", content="SELECT 1")
        assert query.name == "test_query"
        assert query.content == "SELECT 1"


class TestSourceConnection:
    """Tests for SourceConnection data class."""

    def test_source_connection_creation(self):
        """Verify SourceConnection can be created."""
        connection = SourceConnection(type="duckdb", extra={"filename": "data.duckdb"})
        assert connection.type == "duckdb"
        assert connection.extra["filename"] == "data.duckdb"

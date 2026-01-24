"""Tests for Evidence project source classes."""

import dagster as dg

from dagster_evidence.components.sources import (
    BigQueryEvidenceProjectSource,
    DuckdbEvidenceProjectSource,
    EvidenceSourceTranslatorData,
    GSheetsEvidenceProjectSource,
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


class TestSourceDefaults:
    """Tests for source default behavior methods."""

    def test_duckdb_hide_source_asset_default(self):
        """Verify DuckDB sources hide assets by default."""
        assert DuckdbEvidenceProjectSource.get_hide_source_asset_default() is True

    def test_motherduck_hide_source_asset_default(self):
        """Verify MotherDuck sources hide assets by default."""
        assert MotherDuckEvidenceProjectSource.get_hide_source_asset_default() is True

    def test_bigquery_hide_source_asset_default(self):
        """Verify BigQuery sources hide assets by default."""
        assert BigQueryEvidenceProjectSource.get_hide_source_asset_default() is True

    def test_gsheets_hide_source_asset_default(self):
        """Verify Google Sheets sources do not hide assets by default."""
        assert GSheetsEvidenceProjectSource.get_hide_source_asset_default() is False

    def test_duckdb_sensor_enabled_default(self):
        """Verify DuckDB sources have sensors enabled by default."""
        assert DuckdbEvidenceProjectSource.get_source_sensor_enabled_default() is True

    def test_motherduck_sensor_enabled_default(self):
        """Verify MotherDuck sources have sensors enabled by default."""
        assert (
            MotherDuckEvidenceProjectSource.get_source_sensor_enabled_default() is True
        )

    def test_bigquery_sensor_enabled_default(self):
        """Verify BigQuery sources have sensors enabled by default."""
        assert BigQueryEvidenceProjectSource.get_source_sensor_enabled_default() is True

    def test_gsheets_sensor_enabled_default(self):
        """Verify Google Sheets sources have sensors enabled by default."""
        assert GSheetsEvidenceProjectSource.get_source_sensor_enabled_default() is True


class TestExtractDataFromSource:
    """Tests for extract_data_from_source method."""

    def test_duckdb_extract_data_simple_query(self):
        """Test DuckDB source extracts table references from simple query."""
        source_content = SourceContent.from_dict(
            {
                "connection": {
                    "type": "duckdb",
                    "options": {"filename": "test.duckdb"},
                },
                "queries": [{"name": "test", "content": "SELECT * FROM orders"}],
            }
        )
        data = EvidenceSourceTranslatorData(
            source_content=source_content,
            source_group="test_source",
            query=source_content.queries[0],
        )
        extracted = DuckdbEvidenceProjectSource.extract_data_from_source(data)
        assert "table_deps" in extracted
        assert len(extracted["table_deps"]) == 1
        assert extracted["table_deps"][0]["table"] == "orders"

    def test_motherduck_extract_data_with_database(self):
        """Test MotherDuck source extracts table references with database config."""
        source_content = SourceContent.from_dict(
            {
                "connection": {
                    "type": "motherduck",
                    "options": {"database": "analytics", "token": "test"},
                },
                "queries": [{"name": "test", "content": "SELECT * FROM events"}],
            }
        )
        data = EvidenceSourceTranslatorData(
            source_content=source_content,
            source_group="md_source",
            query=source_content.queries[0],
        )
        extracted = MotherDuckEvidenceProjectSource.extract_data_from_source(data)
        assert "table_deps" in extracted
        assert len(extracted["table_deps"]) == 1
        assert extracted["table_deps"][0]["table"] == "events"
        assert extracted["table_deps"][0]["database"] == "analytics"

    def test_bigquery_extract_data_with_project(self):
        """Test BigQuery source extracts table references with project config."""
        source_content = SourceContent.from_dict(
            {
                "connection": {
                    "type": "bigquery",
                    "options": {"project_id": "my-project", "dataset": "analytics"},
                },
                "queries": [{"name": "test", "content": "SELECT * FROM transactions"}],
            }
        )
        data = EvidenceSourceTranslatorData(
            source_content=source_content,
            source_group="bq_source",
            query=source_content.queries[0],
        )
        extracted = BigQueryEvidenceProjectSource.extract_data_from_source(data)
        assert "table_deps" in extracted
        assert len(extracted["table_deps"]) == 1
        assert extracted["table_deps"][0]["table"] == "transactions"
        assert extracted["table_deps"][0]["database"] == "my-project"
        assert extracted["table_deps"][0]["schema"] == "analytics"

    def test_gsheets_extract_data_returns_sheets_config(self):
        """Test Google Sheets source extracts sheets configuration."""
        source_content = SourceContent.from_dict(
            {
                "connection": {
                    "type": "gsheets",
                    "sheets": {
                        "sales_data": {
                            "id": "test_sheet_id",
                            "pages": ["q1", "q2"],
                        }
                    },
                },
                "queries": [{"name": "sales_data/q1", "content": ""}],
            }
        )
        data = EvidenceSourceTranslatorData(
            source_content=source_content,
            source_group="sheets_source",
            query=source_content.queries[0],
        )
        extracted = GSheetsEvidenceProjectSource.extract_data_from_source(data)
        assert "sheets_config" in extracted
        assert "sales_data" in extracted["sheets_config"]


class TestSourceSensorCreation:
    """Tests for source sensor creation."""

    def test_duckdb_sensor_requires_filename(self):
        """Verify DuckDB sensor returns None without filename."""
        source_content = SourceContent.from_dict(
            {
                "connection": {"type": "duckdb", "options": {}},
                "queries": [{"name": "test", "content": "SELECT * FROM orders"}],
            }
        )
        data = EvidenceSourceTranslatorData(
            source_content=source_content,
            source_group="test",
            query=source_content.queries[0],
            extracted_data={"table_deps": [{"table": "orders", "schema": "main"}]},
        )
        asset_key = dg.AssetKey(["test", "test"])
        sensor = DuckdbEvidenceProjectSource.get_source_sensor(data, asset_key)
        assert sensor is None

    def test_duckdb_sensor_requires_table_deps(self):
        """Verify DuckDB sensor returns None without table dependencies."""
        source_content = SourceContent.from_dict(
            {
                "connection": {"type": "duckdb", "options": {"filename": "test.db"}},
                "queries": [{"name": "test", "content": "SELECT 1"}],
            }
        )
        data = EvidenceSourceTranslatorData(
            source_content=source_content,
            source_group="test",
            query=source_content.queries[0],
            extracted_data={"table_deps": []},
        )
        asset_key = dg.AssetKey(["test", "test"])
        sensor = DuckdbEvidenceProjectSource.get_source_sensor(data, asset_key)
        assert sensor is None

    def test_duckdb_sensor_created_with_valid_config(self):
        """Verify DuckDB sensor is created with valid configuration."""
        source_content = SourceContent.from_dict(
            {
                "connection": {"type": "duckdb", "options": {"filename": "test.db"}},
                "queries": [{"name": "test", "content": "SELECT * FROM orders"}],
            }
        )
        data = EvidenceSourceTranslatorData(
            source_content=source_content,
            source_group="my_source",
            query=source_content.queries[0],
            extracted_data={"table_deps": [{"table": "orders", "schema": "main"}]},
        )
        asset_key = dg.AssetKey(["my_source", "test"])
        sensor = DuckdbEvidenceProjectSource.get_source_sensor(data, asset_key)
        assert sensor is not None
        assert isinstance(sensor, dg.SensorDefinition)
        assert sensor.name == "my_source_test_sensor"

    def test_motherduck_sensor_requires_database(self):
        """Verify MotherDuck sensor returns None without database."""
        source_content = SourceContent.from_dict(
            {
                "connection": {"type": "motherduck", "options": {"token": "test"}},
                "queries": [{"name": "test", "content": "SELECT * FROM events"}],
            }
        )
        data = EvidenceSourceTranslatorData(
            source_content=source_content,
            source_group="test",
            query=source_content.queries[0],
            extracted_data={"table_deps": [{"table": "events", "schema": "main"}]},
        )
        asset_key = dg.AssetKey(["test", "test"])
        sensor = MotherDuckEvidenceProjectSource.get_source_sensor(data, asset_key)
        assert sensor is None

    def test_bigquery_sensor_requires_project_id(self):
        """Verify BigQuery sensor returns None without project_id."""
        source_content = SourceContent.from_dict(
            {
                "connection": {"type": "bigquery", "options": {"dataset": "test"}},
                "queries": [{"name": "test", "content": "SELECT * FROM users"}],
            }
        )
        data = EvidenceSourceTranslatorData(
            source_content=source_content,
            source_group="test",
            query=source_content.queries[0],
            extracted_data={"table_deps": [{"table": "users", "schema": "test"}]},
        )
        asset_key = dg.AssetKey(["test", "test"])
        sensor = BigQueryEvidenceProjectSource.get_source_sensor(data, asset_key)
        assert sensor is None

    def test_gsheets_sensor_requires_sheet_id(self):
        """Verify Google Sheets sensor returns None without sheet ID."""
        source_content = SourceContent.from_dict(
            {
                "connection": {"type": "gsheets", "sheets": {"data": {}}},
                "queries": [{"name": "data", "content": ""}],
            }
        )
        data = EvidenceSourceTranslatorData(
            source_content=source_content,
            source_group="test",
            query=source_content.queries[0],
            extracted_data={},
        )
        asset_key = dg.AssetKey(["test", "data"])
        sensor = GSheetsEvidenceProjectSource.get_source_sensor(data, asset_key)
        assert sensor is None


class TestGSheetsSourceType:
    """Tests for Google Sheets source type."""

    def test_gsheets_source_type(self):
        """Verify GSheetsEvidenceProjectSource returns correct type."""
        assert GSheetsEvidenceProjectSource.get_source_type() == "gsheets"

    def test_gsheets_build_queries_from_sheets_config(self):
        """Test building queries from sheets configuration."""
        connection = {
            "type": "gsheets",
            "sheets": {
                "sales_data": {
                    "id": "sheet_id_1",
                    "pages": ["q1_sales", "q2_sales"],
                },
                "inventory": {
                    "id": "sheet_id_2",
                },
            },
        }
        queries = GSheetsEvidenceProjectSource.build_queries_from_sheets_config(
            connection
        )
        assert len(queries) == 3
        query_names = {q["name"] for q in queries}
        assert "sales_data/q1_sales" in query_names
        assert "sales_data/q2_sales" in query_names
        assert "inventory" in query_names

    def test_gsheets_build_queries_empty_sheets(self):
        """Test building queries with no sheets returns empty list."""
        connection = {"type": "gsheets", "sheets": {}}
        queries = GSheetsEvidenceProjectSource.build_queries_from_sheets_config(
            connection
        )
        assert queries == []

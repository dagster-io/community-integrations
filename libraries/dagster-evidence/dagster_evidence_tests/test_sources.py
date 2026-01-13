"""Tests for Evidence project source classes."""

import pytest
import dagster as dg

from dagster_evidence.components.sources import (
    BaseEvidenceProjectSource,
    BigQueryEvidenceProjectSource,
    DuckdbEvidenceProjectSource,
    MotherDuckEvidenceProjectSource,
    SourceContent,
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


class TestResolveSourceType:
    """Tests for resolve_source_type static method."""

    def test_resolve_duckdb_source(self):
        """Verify resolve_source_type returns DuckdbEvidenceProjectSource."""
        source_content = SourceContent.from_dict(
            {
                "connection": SAMPLE_DUCKDB_CONNECTION,
                "queries": SAMPLE_QUERIES,
            }
        )
        source = BaseEvidenceProjectSource.resolve_source_type(source_content)
        assert isinstance(source, DuckdbEvidenceProjectSource)
        assert source.source_content.connection.type == "duckdb"

    def test_resolve_motherduck_source(self):
        """Verify resolve_source_type returns MotherDuckEvidenceProjectSource."""
        source_content = SourceContent.from_dict(
            {
                "connection": SAMPLE_MOTHERDUCK_CONNECTION,
                "queries": [{"name": "events", "content": "SELECT * FROM events"}],
            }
        )
        source = BaseEvidenceProjectSource.resolve_source_type(source_content)
        assert isinstance(source, MotherDuckEvidenceProjectSource)

    def test_resolve_bigquery_source(self):
        """Verify resolve_source_type returns BigQueryEvidenceProjectSource."""
        source_content = SourceContent.from_dict(
            {
                "connection": SAMPLE_BIGQUERY_CONNECTION,
                "queries": [
                    {"name": "transactions", "content": "SELECT * FROM transactions"}
                ],
            }
        )
        source = BaseEvidenceProjectSource.resolve_source_type(source_content)
        assert isinstance(source, BigQueryEvidenceProjectSource)

    def test_resolve_unknown_source_raises(self):
        """Verify resolve_source_type raises NotImplementedError for unknown types."""
        source_content = SourceContent.from_dict(
            {
                "connection": {"type": "unknown_db"},
                "queries": [],
            }
        )
        with pytest.raises(NotImplementedError, match="Unknown source type"):
            BaseEvidenceProjectSource.resolve_source_type(source_content)


class TestSourceAssetSpecs:
    """Tests for asset spec generation from sources."""

    def test_duckdb_source_asset_specs(self):
        """Verify DuckdbEvidenceProjectSource generates correct asset specs."""
        source_content = SourceContent.from_dict(
            {
                "connection": SAMPLE_DUCKDB_CONNECTION,
                "queries": SAMPLE_QUERIES,
            }
        )
        source = DuckdbEvidenceProjectSource(source_content)
        specs = source.get_source_asset_specs("needful_things")

        assert len(specs) == 2

        # Check first spec (orders)
        orders_spec = next(s for s in specs if s.key.path[-1] == "orders")
        assert orders_spec.group_name == "needful_things"
        assert "evidence" in orders_spec.kinds
        assert "source" in orders_spec.kinds
        assert "duckdb" in orders_spec.kinds

        # Check second spec (customers)
        customers_spec = next(s for s in specs if s.key.path[-1] == "customers")
        assert customers_spec.group_name == "needful_things"

    def test_motherduck_source_asset_specs(self):
        """Verify MotherDuckEvidenceProjectSource generates correct asset specs."""
        source_content = SourceContent.from_dict(
            {
                "connection": SAMPLE_MOTHERDUCK_CONNECTION,
                "queries": [{"name": "events", "content": "SELECT * FROM events"}],
            }
        )
        source = MotherDuckEvidenceProjectSource(source_content)
        specs = source.get_source_asset_specs("analytics")

        assert len(specs) == 1
        assert specs[0].key.path[-1] == "events"
        assert specs[0].group_name == "analytics"
        assert "motherduck" in specs[0].kinds

    def test_bigquery_source_asset_specs(self):
        """Verify BigQueryEvidenceProjectSource generates correct asset specs."""
        source_content = SourceContent.from_dict(
            {
                "connection": SAMPLE_BIGQUERY_CONNECTION,
                "queries": [
                    {"name": "transactions", "content": "SELECT * FROM transactions"}
                ],
            }
        )
        source = BigQueryEvidenceProjectSource(source_content)
        specs = source.get_source_asset_specs("warehouse")

        assert len(specs) == 1
        assert specs[0].key.path[-1] == "transactions"
        assert specs[0].group_name == "warehouse"
        assert "bigquery" in specs[0].kinds

    def test_source_with_no_queries(self):
        """Verify source with no queries returns empty list."""
        source_content = SourceContent.from_dict(
            {
                "connection": SAMPLE_DUCKDB_CONNECTION,
                "queries": [],
            }
        )
        source = DuckdbEvidenceProjectSource(source_content)
        specs = source.get_source_asset_specs("empty_source")

        assert len(specs) == 0

    def test_source_asset_spec_is_dagster_asset_spec(self):
        """Verify generated specs are proper Dagster AssetSpec objects."""
        source_content = SourceContent.from_dict(
            {
                "connection": SAMPLE_DUCKDB_CONNECTION,
                "queries": [{"name": "test_query", "content": "SELECT 1"}],
            }
        )
        source = DuckdbEvidenceProjectSource(source_content)
        specs = source.get_source_asset_specs("test_group")

        assert len(specs) == 1
        assert isinstance(specs[0], dg.AssetSpec)
        assert isinstance(specs[0].key, dg.AssetKey)

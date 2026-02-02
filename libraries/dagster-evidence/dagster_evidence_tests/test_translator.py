"""Tests for Evidence project translator."""

import dagster as dg
import pytest

from dagster_evidence.components.sources import (
    BigQueryEvidenceProjectSource,
    DuckdbEvidenceProjectSource,
    EvidenceProjectTranslatorData,
    EvidenceSourceTranslatorData,
    MotherDuckEvidenceProjectSource,
    SourceContent,
    SourceQuery,
)
from dagster_evidence.components.translator import DagsterEvidenceTranslator

# Sample data constants
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


class TestSourceTypeRegistry:
    """Tests for source type registry."""

    def test_default_registry_contains_duckdb(self):
        """Verify default registry contains duckdb."""
        translator = DagsterEvidenceTranslator()
        assert "duckdb" in translator.SOURCE_TYPE_REGISTRY
        assert translator.SOURCE_TYPE_REGISTRY["duckdb"] == DuckdbEvidenceProjectSource

    def test_default_registry_contains_motherduck(self):
        """Verify default registry contains motherduck."""
        translator = DagsterEvidenceTranslator()
        assert "motherduck" in translator.SOURCE_TYPE_REGISTRY
        assert (
            translator.SOURCE_TYPE_REGISTRY["motherduck"]
            == MotherDuckEvidenceProjectSource
        )

    def test_default_registry_contains_bigquery(self):
        """Verify default registry contains bigquery."""
        translator = DagsterEvidenceTranslator()
        assert "bigquery" in translator.SOURCE_TYPE_REGISTRY
        assert (
            translator.SOURCE_TYPE_REGISTRY["bigquery"] == BigQueryEvidenceProjectSource
        )

    def test_get_source_type_registry_returns_registry(self):
        """Verify get_source_type_registry returns the registry."""
        translator = DagsterEvidenceTranslator()
        registry = translator.get_source_type_registry()
        assert registry == translator.SOURCE_TYPE_REGISTRY
        assert "duckdb" in registry
        assert "motherduck" in registry
        assert "bigquery" in registry


class TestGetSourceClass:
    """Tests for get_source_class method."""

    def test_get_duckdb_source_class(self):
        """Verify get_source_class returns DuckdbEvidenceProjectSource."""
        translator = DagsterEvidenceTranslator()
        source_class = translator.get_source_class("duckdb")
        assert source_class == DuckdbEvidenceProjectSource

    def test_get_motherduck_source_class(self):
        """Verify get_source_class returns MotherDuckEvidenceProjectSource."""
        translator = DagsterEvidenceTranslator()
        source_class = translator.get_source_class("motherduck")
        assert source_class == MotherDuckEvidenceProjectSource

    def test_get_bigquery_source_class(self):
        """Verify get_source_class returns BigQueryEvidenceProjectSource."""
        translator = DagsterEvidenceTranslator()
        source_class = translator.get_source_class("bigquery")
        assert source_class == BigQueryEvidenceProjectSource

    def test_get_unknown_source_class_raises(self):
        """Verify get_source_class raises NotImplementedError for unknown types."""
        translator = DagsterEvidenceTranslator()
        with pytest.raises(NotImplementedError, match="Unknown source type"):
            translator.get_source_class("unknown_db")


class TestGetAssetSpecForSource:
    """Tests for get_asset_spec with source translator data."""

    def test_source_asset_spec_key(self):
        """Verify source asset spec has correct key."""
        translator = DagsterEvidenceTranslator()
        source_content = SourceContent.from_dict(
            {"connection": SAMPLE_DUCKDB_CONNECTION, "queries": SAMPLE_QUERIES}
        )
        data = EvidenceSourceTranslatorData(
            source_content=source_content,
            source_group="needful_things",
            query=source_content.queries[0],
        )
        spec = translator.get_asset_spec(data)

        assert spec.key == dg.AssetKey(["needful_things", "orders"])

    def test_source_asset_spec_group_name(self):
        """Verify source asset spec has correct group name."""
        translator = DagsterEvidenceTranslator()
        source_content = SourceContent.from_dict(
            {"connection": SAMPLE_DUCKDB_CONNECTION, "queries": SAMPLE_QUERIES}
        )
        data = EvidenceSourceTranslatorData(
            source_content=source_content,
            source_group="my_source_group",
            query=source_content.queries[0],
        )
        asset_def = translator.get_asset_spec(data)
        assert isinstance(asset_def, dg.AssetsDefinition)

        # AssetsDefinition stores group names by key
        assert asset_def.group_names_by_key[asset_def.key] == "my_source_group"

    def test_source_asset_spec_kinds(self):
        """Verify source asset spec has correct kinds."""
        translator = DagsterEvidenceTranslator()
        source_content = SourceContent.from_dict(
            {"connection": SAMPLE_DUCKDB_CONNECTION, "queries": SAMPLE_QUERIES}
        )
        data = EvidenceSourceTranslatorData(
            source_content=source_content,
            source_group="needful_things",
            query=source_content.queries[0],
        )
        asset_def = translator.get_asset_spec(data)
        assert isinstance(asset_def, dg.AssetsDefinition)

        # Get the spec from the AssetsDefinition to check kinds
        spec = list(asset_def.specs)[0]
        assert "evidence" in spec.kinds
        assert "source" in spec.kinds
        assert "duckdb" in spec.kinds

    def test_motherduck_source_asset_spec_kinds(self):
        """Verify motherduck source asset spec has correct kinds."""
        translator = DagsterEvidenceTranslator()
        source_content = SourceContent.from_dict(
            {
                "connection": SAMPLE_MOTHERDUCK_CONNECTION,
                "queries": [{"name": "events", "content": "SELECT * FROM events"}],
            }
        )
        data = EvidenceSourceTranslatorData(
            source_content=source_content,
            source_group="analytics",
            query=source_content.queries[0],
        )
        asset_def = translator.get_asset_spec(data)
        assert isinstance(asset_def, dg.AssetsDefinition)

        # Get the spec from the AssetsDefinition to check kinds
        spec = list(asset_def.specs)[0]
        assert "motherduck" in spec.kinds

    def test_bigquery_source_asset_spec_kinds(self):
        """Verify bigquery source asset spec has correct kinds."""
        translator = DagsterEvidenceTranslator()
        source_content = SourceContent.from_dict(
            {
                "connection": SAMPLE_BIGQUERY_CONNECTION,
                "queries": [
                    {"name": "transactions", "content": "SELECT * FROM transactions"}
                ],
            }
        )
        data = EvidenceSourceTranslatorData(
            source_content=source_content,
            source_group="warehouse",
            query=source_content.queries[0],
        )
        asset_def = translator.get_asset_spec(data)
        assert isinstance(asset_def, dg.AssetsDefinition)

        # Get the spec from the AssetsDefinition to check kinds
        spec = list(asset_def.specs)[0]
        assert "bigquery" in spec.kinds


class TestGetAssetSpecForProject:
    """Tests for get_asset_spec with project translator data."""

    def test_project_asset_spec_key(self):
        """Verify project asset spec has correct key."""
        translator = DagsterEvidenceTranslator()
        source_content = SourceContent.from_dict(
            {"connection": SAMPLE_DUCKDB_CONNECTION, "queries": SAMPLE_QUERIES}
        )
        data = EvidenceProjectTranslatorData(
            project_name="my_project",
            sources_by_id={"needful_things": source_content},
            source_deps=[dg.AssetKey(["orders"]), dg.AssetKey(["customers"])],
        )
        spec = translator.get_asset_spec(data)

        assert spec.key == dg.AssetKey(["my_project"])

    def test_project_asset_spec_kinds(self):
        """Verify project asset spec has correct kinds."""
        translator = DagsterEvidenceTranslator()
        data = EvidenceProjectTranslatorData(
            project_name="my_project",
            sources_by_id={},
            source_deps=[],
        )
        spec = translator.get_asset_spec(data)
        assert isinstance(spec, dg.AssetSpec)

        assert "evidence" in spec.kinds

    def test_project_asset_spec_deps(self):
        """Verify project asset spec has correct dependencies."""
        translator = DagsterEvidenceTranslator()
        source_deps = [dg.AssetKey(["orders"]), dg.AssetKey(["customers"])]
        data = EvidenceProjectTranslatorData(
            project_name="my_project",
            sources_by_id={},
            source_deps=source_deps,
        )
        spec = translator.get_asset_spec(data)
        assert isinstance(spec, dg.AssetSpec)

        # spec.deps returns AssetDep objects, so extract the asset keys
        dep_keys = [dep.asset_key for dep in spec.deps]
        assert dep_keys == source_deps


class TestCustomTranslator:
    """Tests for custom translator subclassing."""

    def test_custom_translator_extends_registry_via_attribute(self):
        """Verify custom translator can extend source registry via class attribute."""

        class CustomSource(DuckdbEvidenceProjectSource):
            @staticmethod
            def get_source_type() -> str:
                return "custom_db"

        class CustomTranslator(DagsterEvidenceTranslator):
            SOURCE_TYPE_REGISTRY = {
                **DagsterEvidenceTranslator.SOURCE_TYPE_REGISTRY,
                "custom_db": CustomSource,
            }

        translator = CustomTranslator()
        assert "custom_db" in translator.SOURCE_TYPE_REGISTRY
        assert translator.get_source_class("custom_db") == CustomSource
        # Verify original types still work
        assert translator.get_source_class("duckdb") == DuckdbEvidenceProjectSource

    def test_custom_translator_extends_registry_via_method(self):
        """Verify custom translator can extend source registry via method override."""

        class CustomSource(DuckdbEvidenceProjectSource):
            @staticmethod
            def get_source_type() -> str:
                return "custom_db"

        class CustomTranslator(DagsterEvidenceTranslator):
            def get_source_type_registry(self):
                return {
                    **super().get_source_type_registry(),
                    "custom_db": CustomSource,
                }

        translator = CustomTranslator()
        registry = translator.get_source_type_registry()
        assert "custom_db" in registry
        assert translator.get_source_class("custom_db") == CustomSource
        # Verify original types still work
        assert translator.get_source_class("duckdb") == DuckdbEvidenceProjectSource

    def test_custom_translator_overrides_get_asset_spec(self):
        """Verify custom translator can override get_asset_spec for sources.

        Note: Source assets return AssetsDefinition, so customization is done
        by returning a new asset definition rather than modifying an AssetSpec.
        """

        class CustomTranslator(DagsterEvidenceTranslator):
            def get_asset_spec(self, data):
                if isinstance(data, EvidenceSourceTranslatorData):
                    # For source data, create a custom asset with modified key
                    key = dg.AssetKey(
                        ["custom_prefix", data.source_group, data.query.name]
                    )

                    @dg.asset(
                        key=key,
                        group_name=data.source_group,
                        kinds={"evidence", "source", "custom"},
                    )
                    def _custom_source_asset():
                        return dg.MaterializeResult()

                    return _custom_source_asset
                return super().get_asset_spec(data)

        translator = CustomTranslator()
        source_content = SourceContent.from_dict(
            {"connection": SAMPLE_DUCKDB_CONNECTION, "queries": SAMPLE_QUERIES}
        )
        data = EvidenceSourceTranslatorData(
            source_content=source_content,
            source_group="needful_things",
            query=source_content.queries[0],
        )
        asset_def = translator.get_asset_spec(data)

        assert asset_def.key == dg.AssetKey(
            ["custom_prefix", "needful_things", "orders"]
        )

    def test_custom_translator_modifies_project_spec(self):
        """Verify custom translator can modify project asset spec."""

        class CustomTranslator(DagsterEvidenceTranslator):
            def get_asset_spec(self, data):
                result = super().get_asset_spec(data)
                if isinstance(data, EvidenceProjectTranslatorData):
                    # For project data, result is always AssetSpec
                    assert isinstance(result, dg.AssetSpec)
                    return result.replace_attributes(
                        key=result.key.with_prefix("dashboards"),
                    )
                return result

        translator = CustomTranslator()
        data = EvidenceProjectTranslatorData(
            project_name="my_project",
            sources_by_id={},
            source_deps=[],
        )
        spec = translator.get_asset_spec(data)

        assert spec.key == dg.AssetKey(["dashboards", "my_project"])


class TestTranslatorDataClasses:
    """Tests for translator data classes."""

    def test_evidence_source_translator_data_creation(self):
        """Verify EvidenceSourceTranslatorData can be created."""
        source_content = SourceContent.from_dict(
            {"connection": SAMPLE_DUCKDB_CONNECTION, "queries": SAMPLE_QUERIES}
        )
        query = SourceQuery(name="test", content="SELECT 1")
        data = EvidenceSourceTranslatorData(
            source_content=source_content,
            source_group="test_group",
            query=query,
        )
        assert data.source_group == "test_group"
        assert data.query.name == "test"

    def test_evidence_project_translator_data_creation(self):
        """Verify EvidenceProjectTranslatorData can be created."""
        source_content = SourceContent.from_dict(
            {"connection": SAMPLE_DUCKDB_CONNECTION, "queries": SAMPLE_QUERIES}
        )
        data = EvidenceProjectTranslatorData(
            project_name="my_project",
            sources_by_id={"source1": source_content},
            source_deps=[dg.AssetKey(["dep1"])],
        )
        assert data.project_name == "my_project"
        assert "source1" in data.sources_by_id
        assert len(data.source_deps) == 1


class TestTranslatorTypeDispatch:
    """Tests for translator type dispatch."""

    def test_unknown_data_type_raises(self):
        """Verify get_asset_spec raises TypeError for unknown data types."""
        translator = DagsterEvidenceTranslator()
        with pytest.raises(TypeError, match="Unknown data type"):
            translator.get_asset_spec("invalid_data")  # type: ignore

"""Tests for EvidenceProjectComponentV2."""

import asyncio
from unittest.mock import MagicMock

import dagster as dg

from dagster_evidence.components.evidence_project_v2 import EvidenceProjectComponentV2
from dagster_evidence.components.projects import (
    EvidenceProjectData,
    LocalEvidenceProject,
)
from dagster_evidence.components.deployments import CustomEvidenceProjectDeployment
from dagster_evidence.components.sources import SourceContent

# Sample data constants
SAMPLE_DUCKDB_CONNECTION = {
    "name": "needful_things",
    "type": "duckdb",
    "options": {"filename": "data.duckdb"},
}

SAMPLE_QUERIES = [
    {"name": "orders", "content": "SELECT * FROM orders"},
    {"name": "customers", "content": "SELECT * FROM customers"},
]

SAMPLE_SOURCES_DUCKDB: dict[str, SourceContent] = {
    "needful_things": SourceContent.from_dict(
        {
            "connection": SAMPLE_DUCKDB_CONNECTION,
            "queries": SAMPLE_QUERIES,
        }
    )
}


class TestEvidenceProjectComponentV2:
    """Tests for the main Evidence project component."""

    def test_component_class_exists(self):
        """Verify EvidenceProjectComponentV2 class is defined."""
        assert EvidenceProjectComponentV2 is not None

    def test_component_defs_state_config(self, mock_evidence_project):
        """Verify component generates correct defs_state_config."""
        deployment = CustomEvidenceProjectDeployment(deploy_command="echo deploy")
        project = LocalEvidenceProject(
            project_path=str(mock_evidence_project),
            project_deployment=deployment,
        )

        component = EvidenceProjectComponentV2(evidence_project=project)
        config = component.defs_state_config

        assert "EvidenceProjectComponentV2[test_project]" in config.key

    def test_component_write_state_to_path(self, mock_evidence_project, tmp_path):
        """Verify write_state_to_path serializes project data."""
        deployment = CustomEvidenceProjectDeployment(deploy_command="echo deploy")
        project = LocalEvidenceProject(
            project_path=str(mock_evidence_project),
            project_deployment=deployment,
        )

        component = EvidenceProjectComponentV2(evidence_project=project)
        state_path = tmp_path / "state.json"

        # Run async method
        asyncio.run(component.write_state_to_path(state_path))

        # Verify state was written
        assert state_path.exists()
        content = state_path.read_text()
        assert "test_project" in content
        assert "needful_things" in content

    def test_component_build_defs_from_state_none(self, mock_evidence_project):
        """Verify build_defs_from_state returns empty defs when state is None."""
        deployment = CustomEvidenceProjectDeployment(deploy_command="echo deploy")
        project = LocalEvidenceProject(
            project_path=str(mock_evidence_project),
            project_deployment=deployment,
        )

        component = EvidenceProjectComponentV2(evidence_project=project)
        mock_context = MagicMock()

        defs = component.build_defs_from_state(mock_context, state_path=None)

        assert isinstance(defs, dg.Definitions)
        # Empty definitions should have no assets
        assets = list(defs.resolve_asset_graph().get_all_asset_keys())
        assert len(assets) == 0

    def test_component_build_defs_from_state_with_data(
        self, mock_evidence_project, tmp_path
    ):
        """Verify build_defs_from_state loads assets from state file."""
        deployment = CustomEvidenceProjectDeployment(deploy_command="echo deploy")
        project = LocalEvidenceProject(
            project_path=str(mock_evidence_project),
            project_deployment=deployment,
        )

        component = EvidenceProjectComponentV2(evidence_project=project)

        # Create state file with project data
        project_data = EvidenceProjectData(
            project_name="test_project",
            sources_by_id=SAMPLE_SOURCES_DUCKDB,
        )
        state_path = tmp_path / "state.json"
        state_path.write_text(dg.serialize_value(project_data))

        mock_context = MagicMock()

        defs = component.build_defs_from_state(mock_context, state_path=state_path)

        assert isinstance(defs, dg.Definitions)
        # Should have assets: 2 source assets + 1 main build asset + 2 external dep assets
        assets = list(defs.resolve_asset_graph().get_all_asset_keys())
        assert len(assets) == 5

    def test_component_includes_pipes_client_resource(
        self, mock_evidence_project, tmp_path
    ):
        """Verify component includes PipesSubprocessClient resource."""
        deployment = CustomEvidenceProjectDeployment(deploy_command="echo deploy")
        project = LocalEvidenceProject(
            project_path=str(mock_evidence_project),
            project_deployment=deployment,
        )

        component = EvidenceProjectComponentV2(evidence_project=project)

        # Create state file
        project_data = EvidenceProjectData(
            project_name="test_project",
            sources_by_id=SAMPLE_SOURCES_DUCKDB,
        )
        state_path = tmp_path / "state.json"
        state_path.write_text(dg.serialize_value(project_data))

        mock_context = MagicMock()
        defs = component.build_defs_from_state(mock_context, state_path=state_path)

        # Verify pipes_subprocess_client is in resources
        assert defs.resources is not None
        assert "pipes_subprocess_client" in defs.resources
        assert isinstance(
            defs.resources["pipes_subprocess_client"], dg.PipesSubprocessClient
        )


class TestComponentAssetGeneration:
    """Tests for asset generation from component."""

    def test_generated_assets_have_correct_keys(self, mock_evidence_project, tmp_path):
        """Verify generated assets have expected keys."""
        deployment = CustomEvidenceProjectDeployment(deploy_command="echo deploy")
        project = LocalEvidenceProject(
            project_path=str(mock_evidence_project),
            project_deployment=deployment,
        )

        component = EvidenceProjectComponentV2(evidence_project=project)

        project_data = EvidenceProjectData(
            project_name="test_project",
            sources_by_id=SAMPLE_SOURCES_DUCKDB,
        )
        state_path = tmp_path / "state.json"
        state_path.write_text(dg.serialize_value(project_data))

        mock_context = MagicMock()
        defs = component.build_defs_from_state(mock_context, state_path=state_path)

        asset_keys = list(defs.resolve_asset_graph().get_all_asset_keys())
        key_paths = [key.path for key in asset_keys]

        # Should have source assets with source group prefix
        assert ("needful_things", "orders") in key_paths or [
            "needful_things",
            "orders",
        ] in key_paths
        assert ("needful_things", "customers") in key_paths or [
            "needful_things",
            "customers",
        ] in key_paths
        # Should have main project asset
        assert ("test_project",) in key_paths or ["test_project"] in key_paths

    def test_main_asset_depends_on_sources(self, mock_evidence_project, tmp_path):
        """Verify main project asset depends on source assets."""
        deployment = CustomEvidenceProjectDeployment(deploy_command="echo deploy")
        project = LocalEvidenceProject(
            project_path=str(mock_evidence_project),
            project_deployment=deployment,
        )

        component = EvidenceProjectComponentV2(evidence_project=project)

        project_data = EvidenceProjectData(
            project_name="test_project",
            sources_by_id=SAMPLE_SOURCES_DUCKDB,
        )
        state_path = tmp_path / "state.json"
        state_path.write_text(dg.serialize_value(project_data))

        mock_context = MagicMock()
        defs = component.build_defs_from_state(mock_context, state_path=state_path)

        graph = defs.resolve_asset_graph()
        main_asset_key = dg.AssetKey(["test_project"])

        # Get dependencies of main asset
        deps = graph.get(main_asset_key).parent_keys
        dep_paths = [key.path for key in deps]

        # Main asset should depend on source assets (with source group prefix)
        assert ("needful_things", "orders") in dep_paths or [
            "needful_things",
            "orders",
        ] in dep_paths
        assert ("needful_things", "customers") in dep_paths or [
            "needful_things",
            "customers",
        ] in dep_paths

    def test_source_assets_have_correct_kinds(self, mock_evidence_project, tmp_path):
        """Verify source assets have evidence and source kinds."""
        deployment = CustomEvidenceProjectDeployment(deploy_command="echo deploy")
        project = LocalEvidenceProject(
            project_path=str(mock_evidence_project),
            project_deployment=deployment,
        )

        component = EvidenceProjectComponentV2(evidence_project=project)

        project_data = EvidenceProjectData(
            project_name="test_project",
            sources_by_id=SAMPLE_SOURCES_DUCKDB,
        )
        state_path = tmp_path / "state.json"
        state_path.write_text(dg.serialize_value(project_data))

        mock_context = MagicMock()
        defs = component.build_defs_from_state(mock_context, state_path=state_path)

        graph = defs.resolve_asset_graph()
        orders_key = dg.AssetKey(["needful_things", "orders"])

        if orders_key in graph.get_all_asset_keys():
            asset_node = graph.get(orders_key)
            # Check kinds contain expected values
            assert "evidence" in asset_node.kinds
            assert "source" in asset_node.kinds
            assert "duckdb" in asset_node.kinds

    def test_main_asset_has_evidence_kind(self, mock_evidence_project, tmp_path):
        """Verify main project asset has evidence kind."""
        deployment = CustomEvidenceProjectDeployment(deploy_command="echo deploy")
        project = LocalEvidenceProject(
            project_path=str(mock_evidence_project),
            project_deployment=deployment,
        )

        component = EvidenceProjectComponentV2(evidence_project=project)

        project_data = EvidenceProjectData(
            project_name="test_project",
            sources_by_id=SAMPLE_SOURCES_DUCKDB,
        )
        state_path = tmp_path / "state.json"
        state_path.write_text(dg.serialize_value(project_data))

        mock_context = MagicMock()
        defs = component.build_defs_from_state(mock_context, state_path=state_path)

        graph = defs.resolve_asset_graph()
        main_key = dg.AssetKey(["test_project"])

        asset_node = graph.get(main_key)
        assert "evidence" in asset_node.kinds


class TestComponentStateLifecycle:
    """Tests for component state management lifecycle."""

    def test_state_write_and_read_cycle(self, mock_evidence_project, tmp_path):
        """Verify full state write/read cycle works correctly."""
        deployment = CustomEvidenceProjectDeployment(deploy_command="echo deploy")
        project = LocalEvidenceProject(
            project_path=str(mock_evidence_project),
            project_deployment=deployment,
        )

        component = EvidenceProjectComponentV2(evidence_project=project)
        state_path = tmp_path / "state.json"

        # Write state
        asyncio.run(component.write_state_to_path(state_path))

        # Read state and build defs
        mock_context = MagicMock()
        defs = component.build_defs_from_state(mock_context, state_path=state_path)

        # Verify assets were created
        asset_keys = list(defs.resolve_asset_graph().get_all_asset_keys())
        assert len(asset_keys) > 0

        # Verify main project asset exists
        key_paths = [key.path for key in asset_keys]
        assert ("test_project",) in key_paths or ["test_project"] in key_paths

    def test_state_file_contains_serialized_data(self, mock_evidence_project, tmp_path):
        """Verify state file contains properly serialized EvidenceProjectData."""
        deployment = CustomEvidenceProjectDeployment(deploy_command="echo deploy")
        project = LocalEvidenceProject(
            project_path=str(mock_evidence_project),
            project_deployment=deployment,
        )

        component = EvidenceProjectComponentV2(evidence_project=project)
        state_path = tmp_path / "state.json"

        asyncio.run(component.write_state_to_path(state_path))

        # Read and deserialize state
        content = state_path.read_text()
        data = dg.deserialize_value(content, EvidenceProjectData)

        assert data.project_name == "test_project"
        assert "needful_things" in data.sources_by_id
        assert data.sources_by_id["needful_things"].connection is not None
        assert data.sources_by_id["needful_things"].queries is not None

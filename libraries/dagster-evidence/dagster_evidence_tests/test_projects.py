"""Tests for Evidence project classes."""

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml
import dagster as dg

from dagster_evidence.components.deployments import CustomEvidenceProjectDeployment
from dagster_evidence.components.projects import (
    BaseEvidenceProject,
    EvidenceProjectData,
    EvidenceStudioProject,
    EvidenceStudioProjectArgs,
    LocalEvidenceProject,
    LocalEvidenceProjectArgs,
    resolve_evidence_project,
)

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

SAMPLE_SOURCES_DUCKDB = {
    "needful_things": {
        "connection": SAMPLE_DUCKDB_CONNECTION,
        "queries": SAMPLE_QUERIES,
    }
}


def create_evidence_project(
    base_path: Path,
    project_name: str = "test_project",
    sources: dict | None = None,
    with_config: bool = True,
    base_path_config: str | None = None,
) -> Path:
    """Create a test Evidence project structure."""
    project_path = base_path / project_name
    project_path.mkdir(parents=True, exist_ok=True)

    package_json = {
        "name": project_name,
        "version": "1.0.0",
        "scripts": {"sources": "evidence sources", "build": "evidence build"},
    }
    with open(project_path / "package.json", "w") as f:
        json.dump(package_json, f)

    if with_config:
        config = {}
        if base_path_config:
            config["deployment"] = {"basePath": base_path_config}
        with open(project_path / "evidence.config.yaml", "w") as f:
            yaml.dump(config, f)

    if sources:
        sources_path = project_path / "sources"
        for source_name, source_config in sources.items():
            source_dir = sources_path / source_name
            source_dir.mkdir(parents=True, exist_ok=True)
            with open(source_dir / "connection.yaml", "w") as f:
                yaml.dump(source_config.get("connection", {}), f)
            for query in source_config.get("queries", []):
                sql_file = source_dir / f"{query['name']}.sql"
                sql_file.write_text(query.get("content", ""))

    return project_path


class TestEvidenceProjectData:
    """Tests for EvidenceProjectData dataclass."""

    def test_evidence_project_data_creation(self, evidence_project_data):
        """Verify EvidenceProjectData can be created with sample data."""
        assert evidence_project_data.project_name == "test_project"
        assert "needful_things" in evidence_project_data.sources_by_id

    def test_evidence_project_data_serialization(self, evidence_project_data):
        """Verify EvidenceProjectData can be serialized and deserialized."""
        serialized = dg.serialize_value(evidence_project_data)
        deserialized = dg.deserialize_value(serialized, EvidenceProjectData)

        assert deserialized.project_name == evidence_project_data.project_name
        assert deserialized.sources_by_id == evidence_project_data.sources_by_id

    def test_evidence_project_data_empty_sources(self):
        """Verify EvidenceProjectData works with empty sources."""
        data = EvidenceProjectData(project_name="empty_project", sources_by_id={})
        assert data.project_name == "empty_project"
        assert data.sources_by_id == {}


class TestLocalEvidenceProjectArgs:
    """Tests for LocalEvidenceProjectArgs."""

    def test_local_project_args_type(self):
        """Verify LocalEvidenceProjectArgs has correct default type."""
        # Note: project_deployment would need to be resolved in practice
        # This tests the basic structure
        assert LocalEvidenceProjectArgs.model_fields["project_type"].default == "local"

    def test_local_project_args_required_fields(self):
        """Verify LocalEvidenceProjectArgs has required project_path."""
        # project_path is required (no default)
        assert LocalEvidenceProjectArgs.model_fields["project_path"].is_required()


class TestEvidenceStudioProjectArgs:
    """Tests for EvidenceStudioProjectArgs."""

    def test_studio_project_args_type(self):
        """Verify EvidenceStudioProjectArgs has correct default type."""
        args = EvidenceStudioProjectArgs(evidence_studio_url="https://studio.evidence.dev/project")
        assert args.project_type == "evidence_studio"
        assert args.evidence_studio_url == "https://studio.evidence.dev/project"

    def test_studio_project_args_default_git_url(self):
        """Verify EvidenceStudioProjectArgs has default git URL."""
        args = EvidenceStudioProjectArgs(evidence_studio_url="https://studio.evidence.dev/project")
        assert args.evidence_project_git_url == "no_url"


class TestLocalEvidenceProject:
    """Tests for LocalEvidenceProject class."""

    def test_local_project_creation(self, mock_evidence_project):
        """Verify LocalEvidenceProject can be created."""
        deployment = CustomEvidenceProjectDeployment(deploy_command="echo deploy")
        project = LocalEvidenceProject(
            project_path=str(mock_evidence_project),
            project_deployment=deployment,
        )
        assert project.project_path == str(mock_evidence_project)
        assert project.npm_executable == "npm"
        assert isinstance(project, BaseEvidenceProject)

    def test_local_project_custom_npm_executable(self, mock_evidence_project):
        """Verify LocalEvidenceProject accepts custom npm executable."""
        deployment = CustomEvidenceProjectDeployment(deploy_command="echo deploy")
        project = LocalEvidenceProject(
            project_path=str(mock_evidence_project),
            project_deployment=deployment,
            npm_executable="/usr/local/bin/npm",
        )
        assert project.npm_executable == "/usr/local/bin/npm"

    def test_local_project_get_name(self, mock_evidence_project):
        """Verify get_evidence_project_name returns directory name."""
        deployment = CustomEvidenceProjectDeployment(deploy_command="echo deploy")
        project = LocalEvidenceProject(
            project_path=str(mock_evidence_project),
            project_deployment=deployment,
        )
        assert project.get_evidence_project_name() == "test_project"

    def test_local_project_parse_sources(self, mock_evidence_project):
        """Verify parse_evidence_project_sources reads source files."""
        deployment = CustomEvidenceProjectDeployment(deploy_command="echo deploy")
        project = LocalEvidenceProject(
            project_path=str(mock_evidence_project),
            project_deployment=deployment,
        )
        sources = project.parse_evidence_project_sources()

        assert "needful_things" in sources
        assert "connection" in sources["needful_things"]
        assert "queries" in sources["needful_things"]

        # Check connection was parsed from YAML
        connection = sources["needful_things"]["connection"]
        assert connection["type"] == "duckdb"

        # Check queries were parsed from SQL files
        queries = sources["needful_things"]["queries"]
        query_names = [q["name"] for q in queries]
        assert "orders" in query_names
        assert "customers" in query_names

    def test_local_project_parse_sources_missing_folder(self, tmp_path):
        """Verify parse_evidence_project_sources raises for missing sources folder."""
        project_path = tmp_path / "no_sources_project"
        project_path.mkdir()

        deployment = CustomEvidenceProjectDeployment(deploy_command="echo deploy")
        project = LocalEvidenceProject(
            project_path=str(project_path),
            project_deployment=deployment,
        )

        with pytest.raises(FileNotFoundError, match="Sources folder not found"):
            project.parse_evidence_project_sources()

    def test_local_project_get_base_path_with_config(self, mock_evidence_project):
        """Verify _get_base_path reads from evidence.config.yaml."""
        deployment = CustomEvidenceProjectDeployment(deploy_command="echo deploy")
        project = LocalEvidenceProject(
            project_path=str(mock_evidence_project),
            project_deployment=deployment,
        )
        base_path = project._get_base_path()
        assert base_path == "build/test-dashboard"

    def test_local_project_get_base_path_no_config(self, mock_evidence_project_no_config):
        """Verify _get_base_path returns 'build' when no config exists."""
        deployment = CustomEvidenceProjectDeployment(deploy_command="echo deploy")
        project = LocalEvidenceProject(
            project_path=str(mock_evidence_project_no_config),
            project_deployment=deployment,
        )
        base_path = project._get_base_path()
        assert base_path == "build"

    def test_local_project_parse_project(self, mock_evidence_project):
        """Verify parse_evidence_project returns EvidenceProjectData."""
        deployment = CustomEvidenceProjectDeployment(deploy_command="echo deploy")
        project = LocalEvidenceProject(
            project_path=str(mock_evidence_project),
            project_deployment=deployment,
        )
        data = project.parse_evidence_project()

        assert isinstance(data, EvidenceProjectData)
        assert data.project_name == "test_project"
        assert "needful_things" in data.sources_by_id

    def test_local_project_load_source_assets(self, evidence_project_data):
        """Verify load_source_assets creates AssetSpecs from project data."""
        deployment = CustomEvidenceProjectDeployment(deploy_command="echo deploy")
        project = LocalEvidenceProject(
            project_path="/fake/path",
            project_deployment=deployment,
        )
        source_assets = project.load_source_assets(evidence_project_data)

        assert len(source_assets) == 2  # orders and customers
        asset_names = [spec.key.path[-1] for spec in source_assets]
        assert "orders" in asset_names
        assert "customers" in asset_names


class TestEvidenceStudioProject:
    """Tests for EvidenceStudioProject class."""

    def test_studio_project_creation(self):
        """Verify EvidenceStudioProject can be created."""
        project = EvidenceStudioProject(
            evidence_studio_url="https://studio.evidence.dev/project"
        )
        assert project.evidence_studio_url == "https://studio.evidence.dev/project"
        assert isinstance(project, BaseEvidenceProject)

    def test_studio_project_not_implemented(self):
        """Verify EvidenceStudioProject methods raise NotImplementedError."""
        project = EvidenceStudioProject(
            evidence_studio_url="https://studio.evidence.dev/project"
        )

        with pytest.raises(NotImplementedError):
            project.parse_evidence_project_sources()

        with pytest.raises(NotImplementedError):
            project.get_evidence_project_name()


class TestResolveProject:
    """Tests for resolve_evidence_project function."""

    def test_resolve_local_project(self, mock_evidence_project, env_with_github_token):
        """Verify resolve_evidence_project handles local type."""
        mock_context = MagicMock()
        mock_context.resolve_source_relative_path.return_value = mock_evidence_project

        model = {
            "project_type": "local",
            "project_path": str(mock_evidence_project),
            "project_deployment": {
                "type": "custom",
                "deploy_command": "echo deploy",
            },
        }

        project = resolve_evidence_project(mock_context, model)

        assert isinstance(project, LocalEvidenceProject)
        assert project.project_path == str(mock_evidence_project)

    def test_resolve_studio_project(self):
        """Verify resolve_evidence_project handles evidence_studio type."""
        mock_context = MagicMock()
        model = {
            "project_type": "evidence_studio",
            "evidence_studio_url": "https://studio.evidence.dev/project",
        }

        project = resolve_evidence_project(mock_context, model)

        assert isinstance(project, EvidenceStudioProject)
        assert project.evidence_studio_url == "https://studio.evidence.dev/project"

    def test_resolve_unknown_project_raises(self):
        """Verify resolve_evidence_project raises for unknown types."""
        mock_context = MagicMock()
        model = {"project_type": "unknown_type"}

        with pytest.raises(NotImplementedError, match="Unknown project type"):
            resolve_evidence_project(mock_context, model)

    def test_resolve_default_local_type(self, mock_evidence_project, env_with_github_token):
        """Verify resolve_evidence_project defaults to local type."""
        mock_context = MagicMock()
        mock_context.resolve_source_relative_path.return_value = mock_evidence_project

        model = {
            # No project_type specified - should default to local
            "project_path": str(mock_evidence_project),
            "project_deployment": {
                "type": "custom",
                "deploy_command": "echo deploy",
            },
        }

        project = resolve_evidence_project(mock_context, model)

        assert isinstance(project, LocalEvidenceProject)


class TestCreateEvidenceProjectHelper:
    """Tests for the create_evidence_project helper function."""

    def test_create_project_with_sources(self, tmp_path):
        """Verify create_evidence_project creates proper structure."""
        project_path = create_evidence_project(
            base_path=tmp_path,
            project_name="helper_test",
            sources=SAMPLE_SOURCES_DUCKDB,
            with_config=True,
            base_path_config="/my-dashboard",
        )

        assert project_path.exists()
        assert (project_path / "package.json").exists()
        assert (project_path / "evidence.config.yaml").exists()
        assert (project_path / "sources" / "needful_things" / "connection.yaml").exists()
        assert (project_path / "sources" / "needful_things" / "orders.sql").exists()

    def test_create_project_without_config(self, tmp_path):
        """Verify create_evidence_project can skip config file."""
        project_path = create_evidence_project(
            base_path=tmp_path,
            project_name="no_config_test",
            sources=SAMPLE_SOURCES_DUCKDB,
            with_config=False,
        )

        assert project_path.exists()
        assert (project_path / "package.json").exists()
        assert not (project_path / "evidence.config.yaml").exists()

    def test_create_project_empty_sources(self, tmp_path):
        """Verify create_evidence_project works with no sources."""
        project_path = create_evidence_project(
            base_path=tmp_path,
            project_name="empty_sources_test",
            sources=None,
            with_config=True,
        )

        assert project_path.exists()
        assert (project_path / "package.json").exists()
        assert not (project_path / "sources").exists()

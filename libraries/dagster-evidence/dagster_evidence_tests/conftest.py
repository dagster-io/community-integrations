"""Shared fixtures and sample data for dagster-evidence tests."""

import json
import os
import shutil
from pathlib import Path
from typing import Iterator
from unittest.mock import MagicMock, patch

import pytest
import yaml

from dagster_evidence.components.projects import EvidenceProjectData
from dagster_evidence.components.sources import SourceContent

# =============================================================================
# Test Paths
# =============================================================================

TEST_DIR = Path(__file__).parent
TEST_PROJECTS_DIR = TEST_DIR / "evidence_projects"
TEST_PROJECT_PATH = TEST_PROJECTS_DIR / "test_project"


# =============================================================================
# Sample Data Constants
# =============================================================================

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

SAMPLE_SOURCES_DUCKDB: dict[str, SourceContent] = {
    "needful_things": SourceContent.from_dict(
        {
            "connection": SAMPLE_DUCKDB_CONNECTION,
            "queries": SAMPLE_QUERIES,
        }
    )
}

SAMPLE_SOURCES_MOTHERDUCK: dict[str, SourceContent] = {
    "analytics": SourceContent.from_dict(
        {
            "connection": SAMPLE_MOTHERDUCK_CONNECTION,
            "queries": [{"name": "events", "content": "SELECT * FROM events"}],
        }
    )
}

SAMPLE_SOURCES_BIGQUERY: dict[str, SourceContent] = {
    "warehouse": SourceContent.from_dict(
        {
            "connection": SAMPLE_BIGQUERY_CONNECTION,
            "queries": [
                {"name": "transactions", "content": "SELECT * FROM transactions"}
            ],
        }
    )
}


# =============================================================================
# Basic Fixtures
# =============================================================================


@pytest.fixture(name="github_token")
def github_token_fixture() -> str:
    """Fake GitHub token for testing."""
    return "ghp_test_token_1234567890abcdef"


@pytest.fixture(name="github_repo")
def github_repo_fixture() -> str:
    """Fake GitHub repo for testing."""
    return "testuser/test-dashboard"


@pytest.fixture(name="workspace_id")
def workspace_id_fixture() -> str:
    """Fake workspace ID for testing."""
    return "test-workspace-123"


# =============================================================================
# Evidence Project Data Fixtures
# =============================================================================


@pytest.fixture(name="evidence_project_data")
def evidence_project_data_fixture() -> EvidenceProjectData:
    """Sample EvidenceProjectData for testing."""
    return EvidenceProjectData(
        project_name="test_project",
        sources_by_id=SAMPLE_SOURCES_DUCKDB,
    )


@pytest.fixture(name="evidence_project_data_motherduck")
def evidence_project_data_motherduck_fixture() -> EvidenceProjectData:
    """Sample EvidenceProjectData with MotherDuck source."""
    return EvidenceProjectData(
        project_name="motherduck_project",
        sources_by_id=SAMPLE_SOURCES_MOTHERDUCK,
    )


@pytest.fixture(name="evidence_project_data_bigquery")
def evidence_project_data_bigquery_fixture() -> EvidenceProjectData:
    """Sample EvidenceProjectData with BigQuery source."""
    return EvidenceProjectData(
        project_name="bigquery_project",
        sources_by_id=SAMPLE_SOURCES_BIGQUERY,
    )


# =============================================================================
# Evidence Project Path Fixtures
# =============================================================================


@pytest.fixture(name="test_project_path")
def test_project_path_fixture() -> Path:
    """Path to the test Evidence project."""
    return TEST_PROJECT_PATH


@pytest.fixture(name="mock_evidence_project")
def mock_evidence_project_fixture(tmp_path: Path) -> Path:
    """Create a temporary copy of the test Evidence project."""
    project_path = tmp_path / "test_project"
    shutil.copytree(TEST_PROJECT_PATH, project_path)
    return project_path


@pytest.fixture(name="mock_evidence_project_no_config")
def mock_evidence_project_no_config_fixture(tmp_path: Path) -> Path:
    """Create a temporary Evidence project without evidence.config.yaml."""
    project_path = tmp_path / "test_project"
    shutil.copytree(TEST_PROJECT_PATH, project_path)
    # Remove the config file
    config_file = project_path / "evidence.config.yaml"
    if config_file.exists():
        config_file.unlink()
    return project_path


# =============================================================================
# Mock Fixtures
# =============================================================================


@pytest.fixture(name="mock_subprocess")
def mock_subprocess_fixture():
    """Mock subprocess.run for npm commands."""
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0)
        yield mock_run


@pytest.fixture(name="mock_pipes_subprocess_client")
def mock_pipes_subprocess_client_fixture():
    """Mock PipesSubprocessClient for asset execution."""
    mock_client = MagicMock()
    mock_invocation = MagicMock()
    mock_invocation.get_results.return_value = iter([])
    mock_client.run.return_value = mock_invocation
    return mock_client


@pytest.fixture(name="mock_git_repo")
def mock_git_repo_fixture():
    """Mock GitPython Repo for deployment tests."""
    with patch("git.Repo") as mock_repo_class:
        mock_repo = MagicMock()
        mock_repo_class.init.return_value = mock_repo

        # Setup remote mock
        mock_remote = MagicMock()
        mock_repo.create_remote.return_value = mock_remote

        # Setup branch mock
        mock_branch = MagicMock()
        mock_repo.create_head.return_value = mock_branch

        # Setup index mock
        mock_repo.index = MagicMock()

        # Setup config writer mock
        mock_config_writer = MagicMock()
        mock_config_writer.set_value.return_value = mock_config_writer
        mock_config_writer.release.return_value = None
        mock_repo.config_writer.return_value = mock_config_writer

        yield mock_repo_class


@pytest.fixture(name="mock_asset_context")
def mock_asset_context_fixture():
    """Mock AssetExecutionContext for asset tests."""
    mock_context = MagicMock()
    mock_context.log = MagicMock()
    return mock_context


# =============================================================================
# Environment Fixtures
# =============================================================================


@pytest.fixture(name="env_with_github_token")
def env_with_github_token_fixture(github_token: str) -> Iterator[None]:
    """Set GITHUB_TOKEN environment variable for tests."""
    original = os.environ.get("GITHUB_TOKEN")
    os.environ["GITHUB_TOKEN"] = github_token
    yield
    if original is not None:
        os.environ["GITHUB_TOKEN"] = original
    else:
        os.environ.pop("GITHUB_TOKEN", None)


@pytest.fixture(name="env_without_github_token")
def env_without_github_token_fixture() -> Iterator[None]:
    """Ensure GITHUB_TOKEN is not set for tests."""
    original = os.environ.get("GITHUB_TOKEN")
    os.environ.pop("GITHUB_TOKEN", None)
    yield
    if original is not None:
        os.environ["GITHUB_TOKEN"] = original


# =============================================================================
# Helper Functions
# =============================================================================


def create_evidence_project(
    base_path: Path,
    project_name: str = "test_project",
    sources: dict | None = None,
    with_config: bool = True,
    base_path_config: str | None = None,
) -> Path:
    """Create a test Evidence project structure.

    Args:
        base_path: Base directory for the project
        project_name: Name of the project directory
        sources: Dictionary of source configurations
        with_config: Whether to include evidence.config.yaml
        base_path_config: Optional basePath for deployment config

    Returns:
        Path to the created project
    """
    project_path = base_path / project_name
    project_path.mkdir(parents=True, exist_ok=True)

    # Create package.json
    package_json = {
        "name": project_name,
        "version": "1.0.0",
        "scripts": {
            "sources": "evidence sources",
            "build": "evidence build",
        },
    }
    with open(project_path / "package.json", "w") as f:
        json.dump(package_json, f)

    # Create evidence.config.yaml if requested
    if with_config:
        config = {}
        if base_path_config:
            config["deployment"] = {"basePath": base_path_config}
        with open(project_path / "evidence.config.yaml", "w") as f:
            yaml.dump(config, f)

    # Create sources directory and files
    if sources:
        sources_path = project_path / "sources"
        for source_name, source_config in sources.items():
            source_dir = sources_path / source_name
            source_dir.mkdir(parents=True, exist_ok=True)

            # Write connection.yaml
            with open(source_dir / "connection.yaml", "w") as f:
                yaml.dump(source_config.get("connection", {}), f)

            # Write SQL files
            for query in source_config.get("queries", []):
                sql_file = source_dir / f"{query['name']}.sql"
                sql_file.write_text(query.get("content", ""))

    return project_path

from unittest.mock import patch
import pytest
import dagster as dg
from dagster._core.execution.context.init import build_init_resource_context
from dagster._utils.test import wrap_op_in_graph_and_execute
from dagster_evidence import EvidenceResource, load_evidence_asset_specs, EvidenceFilter
import subprocess
import json


@pytest.fixture
def mock_evidence_project(tmp_path):
    """Create a mock Evidence project structure."""
    project_path = tmp_path / "evidence-project"
    project_path.mkdir()

    # Define directory structure
    directories = [
        "pages",
        "pages/marketing-dashboard",
        "pages/finance-dashboard",
        "pages/ops-dashboard",
    ]

    # Create directories
    for dir_path in directories:
        (project_path / dir_path).mkdir()

    # Define dashboard files
    dashboard_files = [
        "pages/marketing-dashboard/funnel.md",
        "pages/marketing-dashboard/retention.md",
        "pages/finance-dashboard/revenue-trends.md",
        "pages/ops-dashboard/on-time-delivery.md",
    ]

    # Create dashboard files
    for file_path in dashboard_files:
        (project_path / file_path).touch()

    # Create package.json
    package_json = {
        "name": "evidence-project",
        "version": "1.0.0",
        "scripts": {"sources": "evidence sources", "build": "evidence build"},
        "dependencies": {"@evidence-dev/evidence": "^0.1.0"},
    }
    with open(project_path / "package.json", "w") as f:
        json.dump(package_json, f)

    return str(project_path)


@pytest.fixture
def mock_subprocess_run():
    with patch("subprocess.run") as mock_run:
        yield mock_run


@pytest.fixture
def evidence_resource(mock_evidence_project):
    return EvidenceResource(
        project_path=mock_evidence_project,
        deploy_command="echo 'deploy'",
        npm_executable="npm",
    )


def test_evidence_resource_setup(evidence_resource):
    evidence_resource.setup_for_execution(build_init_resource_context())
    assert evidence_resource.project_path == evidence_resource.project_path
    assert evidence_resource.deploy_command == "echo 'deploy'"
    assert evidence_resource.npm_executable == "npm"


@patch("dagster.OpExecutionContext", autospec=dg.OpExecutionContext)
def test_evidence_resource_with_op(
    mock_context, mock_subprocess_run, evidence_resource
):
    @dg.op
    def evidence_op(evidence_resource: EvidenceResource):
        assert evidence_resource
        evidence_resource.build()

    result = wrap_op_in_graph_and_execute(
        evidence_op,
        resources={"evidence_resource": evidence_resource},
    )
    assert result.success
    assert mock_subprocess_run.call_count == 2


@patch("dagster.AssetExecutionContext", autospec=dg.AssetExecutionContext)
def test_evidence_resource_with_asset(
    mock_context, mock_subprocess_run, evidence_resource
):
    @dg.asset
    def evidence_asset(evidence_resource: EvidenceResource):
        assert evidence_resource
        evidence_resource.build()

    result = dg.materialize_to_memory(
        [evidence_asset],
        resources={"evidence_resource": evidence_resource},
    )
    assert result.success
    assert mock_subprocess_run.call_count == 2


@patch("dagster.AssetExecutionContext", autospec=dg.AssetExecutionContext)
def test_evidence_resource_with_multi_asset(
    mock_context, mock_subprocess_run, evidence_resource
):
    @dg.multi_asset(
        specs=[dg.AssetSpec("build"), dg.AssetSpec("deploy")],
    )
    def evidence_multi_asset(evidence_resource: EvidenceResource):
        assert evidence_resource
        evidence_resource.build()
        return None, None

    result = dg.materialize_to_memory(
        [evidence_multi_asset],
        resources={"evidence_resource": evidence_resource},
    )
    assert result.success
    assert mock_subprocess_run.call_count == 2


def test_evidence_resource_error_handling(evidence_resource, mock_subprocess_run):
    mock_subprocess_run.side_effect = subprocess.CalledProcessError(
        1, "npm run sources"
    )

    with pytest.raises(subprocess.CalledProcessError):
        evidence_resource.build()


def test_load_evidence_asset_specs(mock_evidence_project):
    """Test that load_evidence_asset_specs returns the correct asset specs."""
    resource = EvidenceResource(
        project_path=mock_evidence_project, deploy_command="echo 'deploy'"
    )
    specs = load_evidence_asset_specs(resource)

    assert len(specs) == 1
    asset_def = specs[0]
    assert len(list(asset_def.specs)) == 5

    asset_keys = {spec.key for spec in asset_def.specs}
    expected_keys = {
        dg.AssetKey(["funnel"]),
        dg.AssetKey(["retention"]),
        dg.AssetKey(["revenue-trends"]),
        dg.AssetKey(["on-time-delivery"]),
        dg.AssetKey(["evidence_build"]),
    }
    assert asset_keys == expected_keys


def test_load_evidence_asset_specs_with_filter(mock_evidence_project):
    """Test that load_evidence_asset_specs respects the EvidenceFilter."""
    resource = EvidenceResource(
        project_path=mock_evidence_project, deploy_command="echo 'deploy'"
    )

    filter = EvidenceFilter(dashboard_directories=["marketing-dashboard"])
    specs = load_evidence_asset_specs(resource, evidence_filter=filter)
    assert len(specs) == 1
    asset_def = specs[0]
    assert len(list(asset_def.specs)) == 3

    # Verify filtered assets
    asset_keys = {spec.key for spec in asset_def.specs}
    expected_keys = {
        dg.AssetKey(["funnel"]),
        dg.AssetKey(["retention"]),
        dg.AssetKey(["evidence_build"]),
    }
    assert asset_keys == expected_keys

    multi_filter = EvidenceFilter(
        dashboard_directories=["marketing-dashboard", "finance-dashboard"]
    )
    multi_specs = load_evidence_asset_specs(resource, evidence_filter=multi_filter)
    assert len(multi_specs) == 1
    asset_def_multi = multi_specs[0]
    assert len(list(asset_def_multi.specs)) == 4

    multi_asset_keys = {spec.key for spec in asset_def_multi.specs}
    multi_expected_keys = {
        dg.AssetKey(["funnel"]),
        dg.AssetKey(["retention"]),
        dg.AssetKey(["revenue-trends"]),
        dg.AssetKey(["evidence_build"]),
    }
    assert multi_asset_keys == multi_expected_keys

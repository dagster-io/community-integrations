from unittest.mock import patch
import pytest
import dagster as dg
from dagster._core.execution.context.init import build_init_resource_context
from dagster._utils.test import wrap_op_in_graph_and_execute
from dagster_evidence import EvidenceResource, load_evidence_asset_specs, EvidenceFilter
import subprocess
import json
import os


@pytest.fixture
def mock_evidence_project(tmp_path):
    """Create a mock Evidence project structure."""
    project_path = tmp_path / "evidence-project"
    project_path.mkdir()

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
    assert mock_subprocess_run.call_count == 1


@patch("dagster.AssetExecutionContext", autospec=dg.AssetExecutionContext)
def test_evidence_resource_with_asset(
    mock_context, mock_subprocess_run, evidence_resource
):
    @dg.asset
    def evidence_application(evidence_resource: EvidenceResource):
        assert evidence_resource
        evidence_resource.build()

    result = dg.materialize_to_memory(
        [evidence_application],
        resources={"evidence_resource": evidence_resource},
    )
    assert result.success
    assert mock_subprocess_run.call_count == 1
    mock_subprocess_run.assert_called_with(
        ["npm", "run", "build"],
        cwd=evidence_resource.project_path,
        check=True,
        capture_output=False,
        env=os.environ,
    )


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
    assert mock_subprocess_run.call_count == 1


def test_evidence_resource_with_sources(mock_evidence_project, mock_subprocess_run):
    """Test that the evidence resource can run sources."""
    resource = EvidenceResource(
        project_path=mock_evidence_project,
        deploy_command="echo 'deploy'",
    )

    # Test running a single source
    resource.sources(source="marketing-dashboard")
    mock_subprocess_run.assert_called_with(
        ["npm", "run", "sources", "--sources=marketing-dashboard"],
        cwd=mock_evidence_project,
        check=True,
        capture_output=False,
        env=os.environ,
    )

    # Test running all sources
    resource.sources()
    mock_subprocess_run.assert_called_with(
        ["npm", "run", "sources"],
        cwd=mock_evidence_project,
        check=True,
        capture_output=False,
        env=os.environ,
    )


def test_evidence_resource_error_handling(evidence_resource, mock_subprocess_run):
    mock_subprocess_run.side_effect = subprocess.CalledProcessError(1, "npm run build")

    with pytest.raises(subprocess.CalledProcessError):
        evidence_resource.build()


def test_load_evidence_asset_specs_with_filter(
    mock_evidence_project, mock_subprocess_run
):
    """Test that load_evidence_asset_specs creates assets that run sources."""
    resource = EvidenceResource(
        project_path=mock_evidence_project,
        deploy_command="echo 'deploy'",
    )

    filter = EvidenceFilter(sources=["marketing-dashboard"])
    specs = load_evidence_asset_specs(resource, evidence_filter=filter)
    assert len(specs) == 1
    asset_def = specs[0]
    assert len(list(asset_def.specs)) == 1

    # Verify filtered assets
    asset_keys = {spec.key for spec in asset_def.specs}
    expected_keys = {
        dg.AssetKey(["marketing-dashboard"]),
    }
    assert asset_keys == expected_keys

    # Test running the assets
    result = dg.materialize_to_memory(
        specs,
        resources={"evidence_resource": resource},
    )
    assert result.success

    # Verify that sources was called with the correct source and build was called
    mock_subprocess_run.assert_any_call(
        ["npm", "run", "sources", "--sources=marketing-dashboard"],
        cwd=mock_evidence_project,
        check=True,
        capture_output=False,
        env=os.environ,
    )
    mock_subprocess_run.assert_any_call(
        ["npm", "run", "build"],
        cwd=mock_evidence_project,
        check=True,
        capture_output=False,
        env=os.environ,
    )


def test_load_evidence_asset_specs_with_multi_filter(
    mock_evidence_project, mock_subprocess_run
):
    """Test that load_evidence_asset_specs creates assets that run sources."""
    resource = EvidenceResource(
        project_path=mock_evidence_project,
        deploy_command="echo 'deploy'",
    )

    multi_filter = EvidenceFilter(sources=["marketing-dashboard", "finance-dashboard"])
    multi_specs = load_evidence_asset_specs(resource, evidence_filter=multi_filter)
    assert len(multi_specs) == 1
    asset_def_multi = multi_specs[0]
    assert len(list(asset_def_multi.specs)) == 2

    multi_asset_keys = {spec.key for spec in asset_def_multi.specs}
    multi_expected_keys = {
        dg.AssetKey(["marketing-dashboard"]),
        dg.AssetKey(["finance-dashboard"]),
    }
    assert multi_asset_keys == multi_expected_keys

    # Test running the multi-source assets
    result = dg.materialize_to_memory(
        multi_specs,
        resources={"evidence_resource": resource},
    )
    assert result.success

    # Verify that sources was called for each source and build was called
    mock_subprocess_run.assert_any_call(
        ["npm", "run", "sources", "--sources=marketing-dashboard"],
        cwd=mock_evidence_project,
        check=True,
        capture_output=False,
        env=os.environ,
    )
    mock_subprocess_run.assert_any_call(
        ["npm", "run", "sources", "--sources=finance-dashboard"],
        cwd=mock_evidence_project,
        check=True,
        capture_output=False,
        env=os.environ,
    )
    mock_subprocess_run.assert_any_call(
        ["npm", "run", "build"],
        cwd=mock_evidence_project,
        check=True,
        capture_output=False,
        env=os.environ,
    )

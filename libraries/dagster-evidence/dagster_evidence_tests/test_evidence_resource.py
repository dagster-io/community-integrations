from unittest.mock import patch
import pytest
import dagster as dg
from dagster._core.execution.context.init import build_init_resource_context
from dagster._utils.test import wrap_op_in_graph_and_execute
from dagster_evidence import EvidenceResource
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
        executable="npm",
    )


def test_evidence_resource_setup(evidence_resource):
    evidence_resource.setup_for_execution(build_init_resource_context())
    assert evidence_resource.project_path == evidence_resource.project_path
    assert evidence_resource.deploy_command == "echo 'deploy'"
    assert evidence_resource.executable == "npm"


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
    mock_subprocess_run.assert_any_call(
        ["npm", "run", "sources"],
        cwd=evidence_resource.project_path,
        check=True,
        capture_output=False,
        env=os.environ,
    )
    mock_subprocess_run.assert_any_call(
        ["npm", "run", "build"],
        cwd=evidence_resource.project_path,
        check=True,
        capture_output=False,
        env=os.environ,
    )


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
    assert mock_subprocess_run.call_count == 2
    mock_subprocess_run.assert_any_call(
        ["npm", "run", "sources"],
        cwd=evidence_resource.project_path,
        check=True,
        capture_output=False,
        env=os.environ,
    )
    mock_subprocess_run.assert_any_call(
        ["npm", "run", "build"],
        cwd=evidence_resource.project_path,
        check=True,
        capture_output=False,
        env=os.environ,
    )


def test_evidence_resource_error_handling(evidence_resource, mock_subprocess_run):
    mock_subprocess_run.side_effect = subprocess.CalledProcessError(
        1, "npm run sources"
    )

    with pytest.raises(subprocess.CalledProcessError):
        evidence_resource.build()

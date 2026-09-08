from unittest.mock import Mock, patch

import pytest
import tenacity
from google.api_core.exceptions import Conflict, ResourceExhausted
from google.cloud.run_v2 import (
    CancelExecutionRequest,
    GetExecutionRequest,
    RunJobRequest,
)

from dagster_contrib_gcp.cloud_run.job_client import CloudRunJobClient, ExecutionStatus


@pytest.fixture
def job_client():
    return CloudRunJobClient(
        run_job_retry_wait=0.01,
        run_job_retry_timeout=0.2,
        jobs_client=Mock(),
        executions_client=Mock(),
    )


def test_resolve_config_value_inline(job_client):
    assert CloudRunJobClient.resolve_config_value("gcp_123") == "gcp_123"


def test_resolve_config_value_env(job_client, monkeypatch):
    monkeypatch.setenv("SOME_GCP_VAR", "resolved_from_env")
    assert (
        CloudRunJobClient.resolve_config_value({"env": "SOME_GCP_VAR"})
        == "resolved_from_env"
    )


def test_resolve_config_value_env_unset(job_client, monkeypatch):
    monkeypatch.delenv("SOME_UNSET_GCP_VAR", raising=False)
    assert CloudRunJobClient.resolve_config_value({"env": "SOME_UNSET_GCP_VAR"}) is None


@patch.object(CloudRunJobClient, "resolve_secret")
def test_resolve_config_value_secret(patched_resolve_secret):
    patched_resolve_secret.return_value = "resolved_from_secret"
    assert (
        CloudRunJobClient.resolve_config_value({"secret_name": "MY_SECRET"})
        == "resolved_from_secret"
    )
    patched_resolve_secret.assert_called_once_with("MY_SECRET")


def test_resolve_config_value_unsupported_dict(job_client):
    with pytest.raises(KeyError):
        CloudRunJobClient.resolve_config_value({"unsupported_key": "value"})


def test_execute_job_request_shape(job_client):
    operation = Mock()
    job_client.jobs_client.run_job.return_value = operation

    result = job_client.execute_job(
        "projects/p/locations/r/jobs/j",
        args=["a", "b"],
        env={"FOO": "bar"},
        container_name="specific_container",
        timeout_seconds=1234,
    )

    assert result is operation
    job_client.jobs_client.run_job.assert_called_once()
    (request,), _ = job_client.jobs_client.run_job.call_args
    assert isinstance(request, RunJobRequest)
    assert request.name == "projects/p/locations/r/jobs/j"
    assert list(request.overrides.container_overrides[0].args) == ["a", "b"]
    assert request.overrides.container_overrides[0].name == "specific_container"
    assert request.overrides.container_overrides[0].env[0].name == "FOO"
    assert request.overrides.container_overrides[0].env[0].value == "bar"
    assert request.overrides.timeout.seconds == 1234


def test_execute_job_retries_then_gives_up_on_resource_exhausted(job_client):
    job_client.jobs_client.run_job.side_effect = ResourceExhausted("quota exceeded")

    # tenacity's stop_after_delay wraps the final failure in a RetryError
    # rather than re-raising the original exception.
    with pytest.raises(tenacity.RetryError):
        job_client.execute_job("projects/p/locations/r/jobs/j")

    assert job_client.jobs_client.run_job.call_count > 1


def test_execute_job_retries_then_succeeds(job_client):
    operation = Mock()
    job_client.jobs_client.run_job.side_effect = [
        ResourceExhausted("quota exceeded"),
        operation,
    ]

    result = job_client.execute_job("projects/p/locations/r/jobs/j")

    assert result is operation
    assert job_client.jobs_client.run_job.call_count == 2


@pytest.mark.parametrize(
    ("execution_kwargs", "expected_status"),
    [
        (
            {
                "reconciling": True,
                "succeeded_count": 0,
                "failed_count": 0,
                "cancelled_count": 0,
            },
            ExecutionStatus.RUNNING,
        ),
        (
            {
                "reconciling": False,
                "succeeded_count": 0,
                "failed_count": 1,
                "cancelled_count": 0,
            },
            ExecutionStatus.FAILED,
        ),
        (
            {
                "reconciling": False,
                "succeeded_count": 0,
                "failed_count": 0,
                "cancelled_count": 1,
            },
            ExecutionStatus.FAILED,
        ),
        (
            {
                "reconciling": False,
                "succeeded_count": 1,
                "failed_count": 0,
                "cancelled_count": 0,
            },
            ExecutionStatus.SUCCESS,
        ),
        (
            {
                "reconciling": False,
                "succeeded_count": 0,
                "failed_count": 0,
                "cancelled_count": 0,
            },
            ExecutionStatus.UNKNOWN,
        ),
    ],
)
def test_get_execution_status(job_client, execution_kwargs, expected_status):
    job_client.executions_client.get_execution.return_value = Mock(**execution_kwargs)

    status = job_client.get_execution_status(
        "projects/p/locations/r/jobs/j/executions/e"
    )

    assert status == expected_status
    (_, kwargs) = job_client.executions_client.get_execution.call_args
    assert isinstance(kwargs["request"], GetExecutionRequest)
    assert kwargs["request"].name == "projects/p/locations/r/jobs/j/executions/e"


def test_get_execution_status_propagates_conflict(job_client):
    job_client.executions_client.get_execution.side_effect = Conflict("conflict")

    with pytest.raises(Conflict):
        job_client.get_execution_status("projects/p/locations/r/jobs/j/executions/e")


def test_cancel_execution_request_shape(job_client):
    job_client.cancel_execution("projects/p/locations/r/jobs/j/executions/e")

    job_client.executions_client.cancel_execution.assert_called_once()
    (_, kwargs) = job_client.executions_client.cancel_execution.call_args
    assert isinstance(kwargs["request"], CancelExecutionRequest)
    assert kwargs["request"].name == "projects/p/locations/r/jobs/j/executions/e"

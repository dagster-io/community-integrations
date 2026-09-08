from contextlib import contextmanager
from typing import ContextManager
from collections.abc import Callable, Iterator
from unittest.mock import Mock, patch

import pytest
from dagster._core.definitions.job_definition import JobDefinition
from dagster._core.instance import DagsterInstance
from dagster._core.storage.dagster_run import DagsterRun
from dagster._core.test_utils import in_process_test_workspace, instance_for_test
from dagster._core.types.loadable_target_origin import LoadableTargetOrigin
from dagster._core.workspace.context import WorkspaceRequestContext
from dagster._core.remote_representation.external import RemoteJob


from dagster_contrib_gcp_tests.cloud_run_tests import repo

IN_PROCESS_NAME = "<<in_process>>"


@pytest.fixture
def instance_cm() -> Callable[..., ContextManager[DagsterInstance]]:
    @contextmanager
    def cm(config=None):
        overrides = {
            "run_launcher": {
                "module": "dagster_contrib_gcp.cloud_run.run_launcher",
                "class": "CloudRunRunLauncher",
                "config": config or {},
            }
        }
        with instance_for_test(overrides) as dagster_instance:
            yield dagster_instance

    return cm


@pytest.fixture
def instance(
    instance_cm: Callable[..., ContextManager[DagsterInstance]],
) -> Iterator[DagsterInstance]:
    with instance_cm(
        {
            "project": "test_project",
            "region": "test_region",
            "job_name_by_code_location": {IN_PROCESS_NAME: "test_job_name"},
            "run_job_retry": {"wait": 1, "timeout": 60},
            "run_timeout": 7200,
        }
    ) as dagster_instance:
        yield dagster_instance


@pytest.fixture
def instance_with_job_configs(
    instance_cm: Callable[..., ContextManager[DagsterInstance]],
) -> Iterator[DagsterInstance]:
    with instance_cm(
        {
            "project": "test_project",
            "region": "test_region",
            "job_name_by_code_location": {
                IN_PROCESS_NAME: {
                    "name": "test_job_with_config",
                    "project_id": "test_gcp-123",
                    "region": "other_test_region",
                }
            },
            "run_job_retry": {"wait": 1, "timeout": 60},
            "run_timeout": 7200,
        }
    ) as dagster_instance:
        yield dagster_instance


@pytest.fixture
def instance_with_multicontainer_job_configs(
    instance_cm: Callable[..., ContextManager[DagsterInstance]],
) -> Iterator[DagsterInstance]:
    with instance_cm(
        {
            "project": "test_project",
            "region": "test_region",
            "job_name_by_code_location": {
                IN_PROCESS_NAME: {
                    "name": "test_job_with_config",
                    "project_id": "test_gcp-123",
                    "region": "other_test_region",
                    "container_name": "specific_container",
                }
            },
            "run_job_retry": {"wait": 1, "timeout": 60},
            "run_timeout": 7200,
        }
    ) as dagster_instance:
        yield dagster_instance


@pytest.fixture
def workspace(instance: DagsterInstance) -> Iterator[WorkspaceRequestContext]:
    with in_process_test_workspace(
        instance,
        loadable_target_origin=LoadableTargetOrigin(
            python_file=repo.__file__,
            attribute=repo.repository.name,
        ),
        container_image="dagster:latest",
    ) as workspace:
        yield workspace


@pytest.fixture
def workspace_with_job_configs(
    instance_with_job_configs: DagsterInstance,
) -> Iterator[WorkspaceRequestContext]:
    with in_process_test_workspace(
        instance_with_job_configs,
        loadable_target_origin=LoadableTargetOrigin(
            python_file=repo.__file__,
            attribute=repo.repository.name,
        ),
        container_image="dagster:latest",
    ) as workspace:
        yield workspace


@pytest.fixture
def workspace_with_multicontainer_job_configs(
    instance_with_multicontainer_job_configs: DagsterInstance,
) -> Iterator[WorkspaceRequestContext]:
    with in_process_test_workspace(
        instance_with_multicontainer_job_configs,
        loadable_target_origin=LoadableTargetOrigin(
            python_file=repo.__file__,
            attribute=repo.repository.name,
        ),
        container_image="dagster:latest",
    ) as workspace:
        yield workspace


@pytest.fixture
def job() -> JobDefinition:
    return repo.job


@pytest.fixture(name="code_location", scope="module")
def code_location_fixture(workspace):
    return workspace.get_code_location("repo_loc")


@pytest.fixture
def run(
    instance: DagsterInstance, job: JobDefinition, external_job: RemoteJob
) -> DagsterRun:
    return instance.create_run_for_job(
        job,
        remote_job_origin=external_job.get_remote_origin(),
        job_code_origin=external_job.get_python_origin(),
    )


@pytest.fixture
def run_with_job_configs(
    instance_with_job_configs: DagsterInstance,
    job: JobDefinition,
    external_job: RemoteJob,
) -> DagsterRun:
    return instance_with_job_configs.create_run_for_job(
        job,
        remote_job_origin=external_job.get_remote_origin(),
        job_code_origin=external_job.get_python_origin(),
    )


@pytest.fixture
def run_with_multicontainer_job_configs(
    instance_with_multicontainer_job_configs: DagsterInstance,
    job: JobDefinition,
    external_job: RemoteJob,
) -> DagsterRun:
    return instance_with_multicontainer_job_configs.create_run_for_job(
        job,
        remote_job_origin=external_job.get_remote_origin(),
        job_code_origin=external_job.get_python_origin(),
    )


@pytest.fixture
def executions():
    return {}


@pytest.fixture
def default_polls_before_success():
    """Number of health-check polls before a mock execution automatically
    transitions from reconciling to succeeded."""
    return None


@pytest.fixture
def mock_jobs_client(executions, default_polls_before_success):
    with patch("google.cloud.run_v2.JobsClient") as MockJobsClient:
        mock_jobs_client = MockJobsClient.return_value
        counter = {"n": 0}

        def run_job(request):
            counter["n"] += 1
            # First call gets a fixed id; later calls get a unique suffix so
            # multiple concurrent executions can be tracked independently.
            execution_id = (
                "test_execution_id"
                if counter["n"] == 1
                else f"test_execution_id_{counter['n']}"
            )
            operation = Mock()
            operation.metadata.name = f"{request.name}/executions/{execution_id}"
            executions[execution_id] = Mock(
                reconciling=True,
                succeeded_count=0,
                failed_count=0,
                cancelled_count=0,
                _polls_remaining=default_polls_before_success,
            )
            return operation

        mock_jobs_client.run_job.side_effect = run_job
        yield mock_jobs_client


@pytest.fixture
def mock_executions_client(executions):
    with patch("google.cloud.run_v2.ExecutionsClient") as MockExecutionsClient:
        mock_executions_client = MockExecutionsClient.return_value

        def cancel_execution(request):
            execution_id = request.name.split("/")[-1]
            executions[execution_id].reconciling = False
            executions[execution_id].cancelled_count = 1

        def get_execution(request):
            execution_id = request.name.split("/")[-1]
            execution = executions[execution_id]
            if execution.reconciling and execution._polls_remaining is not None:
                if execution._polls_remaining <= 0:
                    execution.reconciling = False
                    execution.succeeded_count = 1
                else:
                    execution._polls_remaining -= 1
            return execution

        mock_executions_client.cancel_execution.side_effect = cancel_execution
        mock_executions_client.get_execution.side_effect = get_execution

        yield mock_executions_client


@pytest.fixture
def external_job(workspace: WorkspaceRequestContext) -> RemoteJob:
    location = workspace.get_code_location(workspace.code_location_names[0])
    return location.get_repository(repo.repository.name).get_full_job(repo.job.name)

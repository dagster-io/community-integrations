import pytest
from dagster import reconstructable
from dagster._config import process_config, resolve_to_config_type
from dagster._core.execution.api import create_execution_plan
from dagster._core.execution.context.system import PlanData, PlanOrchestrationContext
from dagster._core.execution.context_creation_job import create_context_free_log_manager
from dagster._core.execution.retries import RetryMode
from dagster._core.executor.init import InitExecutorContext
from dagster._core.executor.step_delegating import (
    StepDelegatingExecutor,
    StepHandlerContext,
)
from dagster._core.test_utils import create_run_for_test, instance_for_test
from dagster._grpc.types import ExecuteStepArgs

from dagster_contrib_gcp.cloud_run.executor import (
    CloudRunStepHandler,
    cloud_run_job_executor,
)
from dagster_contrib_gcp.cloud_run.job_client import CloudRunJobClient

from dagster_contrib_gcp_tests.cloud_run_tests import repo

EXECUTOR_RUN_CONFIG = {
    "execution": {
        "config": {
            "project": "test_project",
            "region": "test_region",
            "job_name": "test_job_name",
        }
    }
}


def _build_executor(instance, job_def, executor_config=None):
    config_type = resolve_to_config_type(
        cloud_run_job_executor.config_schema.config_type
    )
    result = process_config(config_type, executor_config or {})
    assert result.success, str(result.errors)
    creation_fn = cloud_run_job_executor.executor_creation_fn
    assert creation_fn is not None
    return creation_fn(
        InitExecutorContext(
            job=job_def,
            executor_def=cloud_run_job_executor,
            executor_config=result.value,
            instance=instance,
        )
    )


def _step_handler_context(
    job_def, dagster_run, instance, executor, step_key, run_config=None
):
    execution_plan = create_execution_plan(job_def, run_config=run_config)
    log_manager = create_context_free_log_manager(instance, dagster_run)

    plan_context = PlanOrchestrationContext(
        plan_data=PlanData(
            job=job_def,
            dagster_run=dagster_run,
            instance=instance,
            execution_plan=execution_plan,
            raise_on_error=True,
            retry_mode=RetryMode.DISABLED,
        ),
        log_manager=log_manager,
        executor=executor,
        output_capture=None,
    )

    execute_step_args = ExecuteStepArgs(
        job_def.get_python_origin(),
        dagster_run.run_id,
        [step_key],
        print_serialized_events=False,
    )

    return StepHandlerContext(
        instance=instance,
        plan_context=plan_context,
        steps=execution_plan.steps,
        execute_step_args=execute_step_args,
    )


@pytest.fixture
def instance():
    with instance_for_test() as dagster_instance:
        yield dagster_instance


@pytest.fixture
def job_client(mock_jobs_client, mock_executions_client):
    return CloudRunJobClient(run_job_retry_wait=1, run_job_retry_timeout=60)


@pytest.fixture
def step_handler(job_client):
    return CloudRunStepHandler(
        job_client=job_client,
        project="test_project",
        region="test_region",
        job_name="test_job_name",
        container_name=None,
        step_timeout=3600,
    )


@pytest.fixture
def split_step_context(instance, step_handler):
    job_def = reconstructable(repo.dynamic_job)
    run = create_run_for_test(
        instance,
        job_name="dynamic_job",
        job_code_origin=job_def.get_python_origin(),
        run_config=EXECUTOR_RUN_CONFIG,
    )
    return _step_handler_context(
        job_def, run, instance, step_handler, "split", run_config=EXECUTOR_RUN_CONFIG
    )


def test_launch_step_request_shape(split_step_context, mock_jobs_client, step_handler):
    list(step_handler.launch_step(split_step_context))

    mock_jobs_client.run_job.assert_called_once()
    (request,), _ = mock_jobs_client.run_job.call_args
    assert (
        request.name == "projects/test_project/locations/test_region/jobs/test_job_name"
    )

    container_override = request.overrides.container_overrides[0]
    env_by_name = {entry.name: entry.value for entry in container_override.env}
    assert "DAGSTER_COMPRESSED_EXECUTE_STEP_ARGS" in env_by_name
    assert env_by_name["DAGSTER_RUN_STEP_KEY"] == "split"
    assert env_by_name["DAGSTER_RUN_JOB_NAME"] == "dynamic_job"

    assert step_handler._executions_by_step[("split", 0)] == (
        "projects/test_project/locations/test_region/jobs/test_job_name"
        "/executions/test_execution_id"
    )


def test_launch_step_uses_container_name(instance, job_client):
    handler = CloudRunStepHandler(
        job_client=job_client,
        project="test_project",
        region="test_region",
        job_name="test_job_name",
        container_name="specific_container",
        step_timeout=3600,
    )
    job_def = reconstructable(repo.dynamic_job)
    run = create_run_for_test(
        instance,
        job_name="dynamic_job",
        job_code_origin=job_def.get_python_origin(),
        run_config=EXECUTOR_RUN_CONFIG,
    )
    context = _step_handler_context(
        job_def, run, instance, handler, "split", run_config=EXECUTOR_RUN_CONFIG
    )

    list(handler.launch_step(context))

    (request,), _ = job_client.jobs_client.run_job.call_args
    assert request.overrides.container_overrides[0].name == "specific_container"


def test_launch_step_event_metadata(split_step_context, step_handler):
    events = list(step_handler.launch_step(split_step_context))

    assert len(events) == 1
    metadata = events[0].engine_event_data.metadata
    assert "Cloud Run execution name" in metadata
    assert metadata["Cloud Run execution name"].text == (
        "projects/test_project/locations/test_region/jobs/test_job_name"
        "/executions/test_execution_id"
    )


def test_check_step_health_missing_execution_is_unhealthy(
    split_step_context, step_handler
):
    result = step_handler.check_step_health(split_step_context)

    assert not result.is_healthy
    assert "No Cloud Run execution recorded" in result.unhealthy_reason


def test_check_step_health_running_is_healthy(split_step_context, step_handler):
    list(step_handler.launch_step(split_step_context))

    result = step_handler.check_step_health(split_step_context)

    assert result.is_healthy


def test_check_step_health_failed_is_unhealthy(
    split_step_context, step_handler, executions, mock_executions_client
):
    list(step_handler.launch_step(split_step_context))
    executions["test_execution_id"].reconciling = False
    executions["test_execution_id"].failed_count = 1

    result = step_handler.check_step_health(split_step_context)

    assert not result.is_healthy
    assert "failed" in result.unhealthy_reason


def test_terminate_step(split_step_context, step_handler, mock_executions_client):
    list(step_handler.launch_step(split_step_context))

    events = list(step_handler.terminate_step(split_step_context))

    assert len(events) == 1
    mock_executions_client.cancel_execution.assert_called_once()
    (_, kwargs) = mock_executions_client.cancel_execution.call_args
    assert kwargs["request"].name == (
        "projects/test_project/locations/test_region/jobs/test_job_name"
        "/executions/test_execution_id"
    )


def test_terminate_step_missing_execution_is_a_noop(
    split_step_context, step_handler, mock_executions_client
):
    events = list(step_handler.terminate_step(split_step_context))

    assert len(events) == 1
    mock_executions_client.cancel_execution.assert_not_called()


def test_multiple_steps_tracked_independently(instance, step_handler, mock_jobs_client):
    dynamic_job_def = reconstructable(repo.dynamic_job)
    simple_job_def = reconstructable(repo.job)

    dynamic_run = create_run_for_test(
        instance,
        job_name="dynamic_job",
        job_code_origin=dynamic_job_def.get_python_origin(),
        run_config=EXECUTOR_RUN_CONFIG,
    )
    simple_run = create_run_for_test(
        instance,
        job_name="job",
        job_code_origin=simple_job_def.get_python_origin(),
    )

    split_context = _step_handler_context(
        dynamic_job_def,
        dynamic_run,
        instance,
        step_handler,
        "split",
        run_config=EXECUTOR_RUN_CONFIG,
    )
    node_context = _step_handler_context(
        simple_job_def, simple_run, instance, step_handler, "node"
    )

    list(step_handler.launch_step(split_context))
    list(step_handler.launch_step(node_context))

    assert len(step_handler._executions_by_step) == 2
    assert (
        step_handler._executions_by_step[("split", 0)]
        != step_handler._executions_by_step[("node", 0)]
    )
    assert step_handler.check_step_health(split_context).is_healthy
    assert step_handler.check_step_health(node_context).is_healthy


def test_cloud_run_job_executor_wires_max_concurrent(
    instance, mock_jobs_client, mock_executions_client
):
    job_def = reconstructable(repo.dynamic_job)
    executor = _build_executor(
        instance,
        job_def,
        executor_config={
            "project": "test_project",
            "region": "test_region",
            "job_name": "test_job_name",
            "max_concurrent": 4,
        },
    )

    assert isinstance(executor, StepDelegatingExecutor)
    assert executor._max_concurrent == 4
    assert isinstance(executor._step_handler, CloudRunStepHandler)


def test_cloud_run_job_executor_defaults_to_unlimited_concurrency(
    instance, mock_jobs_client, mock_executions_client
):
    job_def = reconstructable(repo.dynamic_job)
    executor = _build_executor(
        instance,
        job_def,
        executor_config={
            "project": "test_project",
            "region": "test_region",
            "job_name": "test_job_name",
        },
    )

    assert executor._max_concurrent is None

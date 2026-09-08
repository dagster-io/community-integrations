from collections.abc import Iterator

from dagster import (
    Any as ConfigAny,
    Field,
    IntSource,
    MetadataValue,
    Noneable,
    _check as check,
)
from dagster._core.events import DagsterEvent, EngineEventData
from dagster._core.execution.retries import RetryMode, get_retries_config
from dagster._core.execution.tags import get_tag_concurrency_limits_config
from dagster._core.executor.base import Executor
from dagster._core.executor.init import InitExecutorContext
from dagster._core.executor.step_delegating import (
    CheckStepHealthResult,
    StepDelegatingExecutor,
    StepHandler,
    StepHandlerContext,
)
from dagster._core.definitions.executor_definition import (
    executor,
    multiple_process_executor_requirements,
)
from google.api_core.exceptions import Conflict, ServerError

from dagster_contrib_gcp.cloud_run.job_client import CloudRunJobClient, ExecutionStatus


class CloudRunStepHandler(StepHandler):
    """Launches each step of a Dagster run as its own Google Cloud Run Job execution,
    reusing the same Cloud Run Job resource configured as the run worker for this code
    location (only the container args/env differ: execute_step vs execute_run).
    """

    def __init__(
        self,
        job_client: CloudRunJobClient,
        project: str,
        region: str,
        job_name: str,
        container_name: str | None,
        step_timeout: int,
    ):
        self._job_client = job_client
        self._project = project
        self._region = region
        self._job_name = job_name
        self._container_name = container_name
        self._step_timeout = step_timeout
        self._executions_by_step: dict[tuple[str, int], str] = {} # Maps (step_key, attempt_count) -> execution_name

    @property
    def name(self) -> str:
        return "CloudRunStepHandler"

    def _fully_qualified_job_name(self) -> str:
        return (
            f"projects/{self._project}/locations/{self._region}/jobs/{self._job_name}"
        )

    def _get_step_key(self, step_handler_context: StepHandlerContext) -> str:
        step_keys_to_execute = check.not_none(
            step_handler_context.execute_step_args.step_keys_to_execute
        )
        assert len(step_keys_to_execute) == 1, (
            "Launching multiple steps at once is not currently supported by "
            "CloudRunStepHandler"
        )
        return step_keys_to_execute[0]

    def _get_attempt_count(
        self, step_handler_context: StepHandlerContext, step_key: str
    ) -> int:
        known_state = step_handler_context.execute_step_args.known_state
        if known_state:
            return known_state.get_retry_state().get_attempt_count(step_key)
        return 0

    def launch_step(
        self, step_handler_context: StepHandlerContext
    ) -> Iterator[DagsterEvent]:
        step_key = self._get_step_key(step_handler_context)
        attempt_count = self._get_attempt_count(step_handler_context, step_key)
        run = step_handler_context.dagster_run

        args = step_handler_context.execute_step_args.get_command_args(
            skip_serialized_namedtuple=True
        )
        env = {
            entry["name"]: entry["value"]
            for entry in step_handler_context.execute_step_args.get_command_env()
        }
        env["DAGSTER_RUN_JOB_NAME"] = run.job_name
        env["DAGSTER_RUN_STEP_KEY"] = step_key

        operation = self._job_client.execute_job(
            self._fully_qualified_job_name(),
            args=args,
            env=env,
            container_name=self._container_name,
            timeout_seconds=self._step_timeout,
        )
        execution_id = operation.metadata.name.split("/")[-1]  # ty: ignore
        execution_name = f"{self._fully_qualified_job_name()}/executions/{execution_id}"
        self._executions_by_step[(step_key, attempt_count)] = execution_name

        yield DagsterEvent.step_worker_starting(
            step_handler_context.get_step_context(step_key),
            message=f'Executing step "{step_key}" in Cloud Run execution {execution_id}.',
            metadata={"Cloud Run execution name": MetadataValue.text(execution_name)},
        )

    def check_step_health(
        self, step_handler_context: StepHandlerContext
    ) -> CheckStepHealthResult:
        step_key = self._get_step_key(step_handler_context)
        attempt_count = self._get_attempt_count(step_handler_context, step_key)
        execution_name = self._executions_by_step.get((step_key, attempt_count))

        if not execution_name:
            return CheckStepHealthResult.unhealthy(
                reason=(
                    f"No Cloud Run execution recorded for step {step_key} "
                    f"(attempt {attempt_count})."
                )
            )

        try:
            status = self._job_client.get_execution_status(execution_name)
        except (ServerError, Conflict):
            return CheckStepHealthResult.unhealthy(
                reason=f"Unable to fetch Cloud Run execution status for step {step_key}."
            )

        if status == ExecutionStatus.FAILED:
            return CheckStepHealthResult.unhealthy(
                reason=f"Cloud Run execution {execution_name} for step {step_key} failed."
            )
        return CheckStepHealthResult.healthy()

    def terminate_step(
        self, step_handler_context: StepHandlerContext
    ) -> Iterator[DagsterEvent]:
        step_key = self._get_step_key(step_handler_context)
        attempt_count = self._get_attempt_count(step_handler_context, step_key)
        execution_name = self._executions_by_step.get((step_key, attempt_count))

        if not execution_name:
            yield DagsterEvent.engine_event(
                step_handler_context.get_step_context(step_key),
                message=f"No Cloud Run execution found to terminate for step {step_key}.",
                event_specific_data=EngineEventData(),
            )
            return

        yield DagsterEvent.engine_event(
            step_handler_context.get_step_context(step_key),
            message=f"Cancelling Cloud Run execution {execution_name} for step {step_key}.",
            event_specific_data=EngineEventData(),
        )
        try:
            self._job_client.cancel_execution(execution_name)
        except (ServerError, Conflict):
            # Best-effort: the execution may already have finished or been cancelled.
            pass


def _cloud_run_executor_config_schema():
    return {
        "project": Field(
            ConfigAny,
            is_required=True,
            description=(
                "GCP project ID containing the Cloud Run Job used for step execution. "
                "May be an explicit value, {env: VAR_NAME}, or {secret_name: NAME}."
            ),
        ),
        "region": Field(
            ConfigAny,
            is_required=True,
            description=(
                "GCP region of the Cloud Run Job used for step execution. May be an "
                "explicit value, {env: VAR_NAME}, or {secret_name: NAME}."
            ),
        ),
        "job_name": Field(
            ConfigAny,
            is_required=True,
            description=(
                "Name of the Cloud Run Job to invoke for each step. This should be the "
                "same Cloud Run Job resource configured as the run worker for this code "
                "location (same container image) - only the invocation args differ "
                "(execute_step vs execute_run). May be an explicit value, "
                "{env: VAR_NAME}, or {secret_name: NAME}."
            ),
        ),
        "container_name": Field(
            Noneable(ConfigAny),
            is_required=False,
            default_value=None,
            description=(
                "Name of the specific container to override, for multi-container Cloud "
                "Run Jobs. Matches CloudRunRunLauncher's container_name option."
            ),
        ),
        "run_job_retry": Field(
            {
                "wait": Field(
                    int,
                    is_required=False,
                    default_value=10,
                    description="Number of seconds to wait between retries",
                ),
                "timeout": Field(
                    int,
                    is_required=False,
                    default_value=300,
                    description="Number of seconds to wait before timing out",
                ),
            },
            is_required=False,
            default_value={"wait": 10, "timeout": 300},
            description=(
                "Retry configuration for step run-job requests on ResourceExhausted "
                "(quota) errors."
            ),
        ),
        "step_timeout": Field(
            int,
            is_required=False,
            default_value=3600,
            description="Timeout in seconds for each per-step Cloud Run execution.",
        ),
        "retries": get_retries_config(),
        "max_concurrent": Field(
            Noneable(IntSource),
            is_required=False,
            default_value=None,
            description=(
                "Limits the number of Cloud Run step executions that can be in flight "
                "at once. Defaults to unlimited. This is the primary lever for "
                "controlling concurrency when a job fans out via DynamicOutput into "
                "many mapped steps - e.g. capping concurrent GCP executions when "
                "processing a large backlog of batches."
            ),
        ),
        "tag_concurrency_limits": get_tag_concurrency_limits_config(),
        "check_step_health_interval_seconds": Field(
            IntSource,
            is_required=False,
            default_value=20,
            description="Interval in seconds at which the health of running steps is checked.",
        ),
    }


@executor(
    name="cloud_run_job_executor",
    config_schema=_cloud_run_executor_config_schema(),
    requirements=multiple_process_executor_requirements(),
)
def cloud_run_job_executor(init_context: InitExecutorContext) -> Executor:
    """Executor which launches each step of a run as its own Google Cloud Run Job
    execution, reusing the same Cloud Run Job resource as the run worker.

    To use it, set it as a job's `executor_def`:

    .. code-block:: python

        @job(executor_def=cloud_run_job_executor)
        def my_job(): ...

    Then configure it with run config:

    .. code-block:: yaml

        execution:
          config:
            project: {env: GOOGLE_CLOUD_PROJECT}
            region: {env: GOOGLE_CLOUD_REGION}
            job_name: my-cloud-run-job-1
            max_concurrent: 4

    `max_concurrent` limits the number of Cloud Run step executions that run
    concurrently for one run - this is the primary lever for controlling how many
    Cloud Run executions get spawned at once when a job fans out into many batches.
    """
    cfg = init_context.executor_config
    run_job_retry = check.dict_elem(cfg, "run_job_retry")
    job_client = CloudRunJobClient(
        run_job_retry_wait=check.int_elem(run_job_retry, "wait"),
        run_job_retry_timeout=check.int_elem(run_job_retry, "timeout"),
    )
    container_name = cfg.get("container_name")
    step_handler = CloudRunStepHandler(
        job_client=job_client,
        project=CloudRunJobClient.resolve_config_value(cfg["project"]),
        region=CloudRunJobClient.resolve_config_value(cfg["region"]),
        job_name=CloudRunJobClient.resolve_config_value(cfg["job_name"]),
        container_name=(
            CloudRunJobClient.resolve_config_value(container_name)
            if container_name is not None
            else None
        ),
        step_timeout=cfg["step_timeout"],
    )
    return StepDelegatingExecutor(
        step_handler,
        retries=RetryMode.from_config(cfg["retries"]),
        max_concurrent=cfg.get("max_concurrent"),
        tag_concurrency_limits=cfg.get("tag_concurrency_limits"),
        check_step_health_interval_seconds=cfg.get(
            "check_step_health_interval_seconds", 20
        ),
    )

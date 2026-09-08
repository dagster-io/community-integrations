import os
from collections.abc import Sequence
from enum import Enum
from typing import Any, Optional

import tenacity
from dagster import _check as check
from google.api_core.exceptions import ResourceExhausted
from google.api_core.operation import Operation
from google.cloud import run_v2
from google.cloud.run_v2 import RunJobRequest
from google.cloud.run_v2.types import k8s_min
from google.cloud.secretmanager_v1 import (
    AccessSecretVersionRequest,
    SecretManagerServiceClient,
)

ENV_KEY = "env"
SECRETS_KEY = "secret_name"


class ExecutionStatus(Enum):
    """Status of a Cloud Run Job execution."""

    RUNNING = "RUNNING"
    FAILED = "FAILED"
    SUCCESS = "SUCCESS"
    UNKNOWN = "UNKNOWN"


class CloudRunJobClient:
    """Google Cloud Run Admin API client: runs a Job execution, resolves config
    values, polls execution status, and cancels executions."""

    def __init__(
        self,
        run_job_retry_wait: int,
        run_job_retry_timeout: int,
        jobs_client: "run_v2.JobsClient | None" = None,
        executions_client: "run_v2.ExecutionsClient | None" = None,
    ):
        self.run_job_retry_wait = run_job_retry_wait
        self.run_job_retry_timeout = run_job_retry_timeout
        self.jobs_client = jobs_client or run_v2.JobsClient()
        self.executions_client = executions_client or run_v2.ExecutionsClient()

    @staticmethod
    def resolve_secret(secret_name: str) -> Any:
        client = SecretManagerServiceClient()
        latest = AccessSecretVersionRequest(name=secret_name)
        response = client.access_secret_version(latest)
        return response.payload.data.decode("UTF-8")

    @classmethod
    def resolve_config_value(cls, value: Any) -> Any:
        """Resolves a config value that may be an explicit value, an
        {"env": VAR_NAME} reference, or a {"secret_name": NAME} reference."""
        try:
            node_config = check.dict_param(value, "value")
        except check.ParameterCheckError:
            # Explicit value
            return value

        if ENV_KEY in node_config:
            env_var = node_config[ENV_KEY]
            return os.getenv(env_var) if env_var is not None else None
        elif SECRETS_KEY in node_config:
            return cls.resolve_secret(node_config[SECRETS_KEY])
        else:
            raise KeyError(
                "Unsupported configuration value. Expected an explicit value, "
                "{'env': ...}, or {'secret_name': ...}."
            )

    def execute_job(
        self,
        fully_qualified_job_name: str,
        args: Sequence[str] | None = None,
        env: Optional["dict[str, str]"] = None,
        container_name: str | None = None,
        timeout_seconds: int | None = None,
    ) -> Operation:
        request = RunJobRequest(name=fully_qualified_job_name)

        overrides = {}
        if args:
            overrides["args"] = args
        if env:
            overrides["env"] = [
                k8s_min.EnvVar(name=name, value=value) for name, value in env.items()
            ]
        if container_name:
            overrides["name"] = container_name

        container_overrides = [RunJobRequest.Overrides.ContainerOverride(**overrides)]

        request.overrides.container_overrides.extend(container_overrides)
        if timeout_seconds is not None:
            request.overrides.timeout = f"{timeout_seconds}s"  # ty: ignore

        @tenacity.retry(
            wait=tenacity.wait_fixed(self.run_job_retry_wait),
            stop=tenacity.stop_after_delay(self.run_job_retry_timeout),
            retry=tenacity.retry_if_exception_type(ResourceExhausted),
        )
        def run_job_with_retries_when_quota_exceeded(request: RunJobRequest):
            return self.jobs_client.run_job(request)

        return run_job_with_retries_when_quota_exceeded(request)

    def get_execution_status(
        self, fully_qualified_execution_name: str
    ) -> ExecutionStatus:
        request = run_v2.GetExecutionRequest(name=fully_qualified_execution_name)
        execution = self.executions_client.get_execution(request=request)
        if execution.reconciling:
            return ExecutionStatus.RUNNING
        elif execution.failed_count > 0 or execution.cancelled_count > 0:
            return ExecutionStatus.FAILED
        elif execution.succeeded_count > 0:
            return ExecutionStatus.SUCCESS
        else:
            return ExecutionStatus.UNKNOWN

    def cancel_execution(self, fully_qualified_execution_name: str) -> None:
        request = run_v2.CancelExecutionRequest(name=fully_qualified_execution_name)
        self.executions_client.cancel_execution(request=request)

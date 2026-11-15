"""Run launcher that starts each Dagster run in a Cloudflare Container.

The launcher does not talk to the Cloudflare API. It talks to a small Worker
(see ``worker/`` in this package's repository directory) that the user deploys
to their own account. The Worker keeps one Durable Object per run; the Durable
Object starts the container, watches it, and reports its state.

Worker HTTP contract (all requests carry ``Authorization: Bearer <token>``):

- ``POST /runs/<run_id>`` with ``{"command", "env", "image", "instance"}``
  starts the run. Repeating the same request does not start a second
  container. Returns ``{"state": ...}``.
- ``GET /runs/<run_id>`` returns ``{"state", "message", "exitCode"}``.
- ``DELETE /runs/<run_id>`` stops the container. Returns ``{"terminated"}``.

States: ``starting``, ``running``, ``canceling``, ``success``, ``failed``,
``canceled``, and ``missing`` for a run ID the Worker does not know.
"""

import http.client
import json
import os
import urllib.error
import urllib.request
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import quote, urlsplit

from dagster import (
    Field,
    IntSource,
    Noneable,
    ScalarUnion,
    StringSource,
)
from dagster import (
    _check as check,
)
from dagster._core.events import EngineEventData
from dagster._core.launcher.base import (
    CheckRunHealthResult,
    LaunchRunContext,
    RunLauncher,
    WorkerStatus,
)
from dagster._core.storage.dagster_run import DagsterRun
from dagster._grpc.types import ExecuteRunArgs
from dagster._serdes import ConfigurableClass, ConfigurableClassData

INSTANCE_TAG = "dagster-cloudflare/instance"
IMAGE_TAG = "dagster-cloudflare/image"

_LOOPBACK_HOSTS = ("localhost", "127.0.0.1", "::1")
_ACTIVE_STATES = ("starting", "running", "canceling")
_STATUS_BY_STATE = {
    "starting": WorkerStatus.RUNNING,
    "running": WorkerStatus.RUNNING,
    "canceling": WorkerStatus.RUNNING,
    "success": WorkerStatus.SUCCESS,
    "failed": WorkerStatus.FAILED,
    "canceled": WorkerStatus.FAILED,
    "missing": WorkerStatus.NOT_FOUND,
}
# Linux reports a process killed by SIGKILL as 128 + 9. The container runtime
# kills the main process this way when it uses more memory than its instance.
_SIGKILL_EXIT_CODE = 137

# URLError and TimeoutError are OSError; a dropped connection is HTTPException.
_TRANSPORT_ERRORS = (OSError, http.client.HTTPException)

InstanceSpec = str | Mapping[str, Any]


class CloudflareWorkerError(Exception):
    """The dispatcher Worker returned an error or could not be reached."""


def _parse_instance(value: Any) -> InstanceSpec | None:
    """Accept an instance type name or a ``{vcpu, memoryMib, diskMb}`` object.

    Run tags are strings, so a custom size in a tag is JSON.
    """
    if value is None or isinstance(value, Mapping):
        return value
    text = check.str_param(value, "instance").strip()
    if text.startswith("{"):
        parsed = json.loads(text)
        check.invariant(
            isinstance(parsed, dict),
            f"Instance JSON must be an object, got {text!r}",
        )
        return parsed
    return text


def _env_from_names(env_vars: Sequence[str]) -> dict[str, str]:
    """Same rules as the Docker and ECS launchers: ``KEY`` copies the value
    from the launcher's environment; ``KEY=VALUE`` sets it."""
    env: dict[str, str] = {}
    for entry in env_vars:
        if "=" in entry:
            key, value = entry.split("=", 1)
            env[key] = value
        elif entry in os.environ:
            env[entry] = os.environ[entry]
        else:
            raise check.CheckError(
                f"Environment variable {entry!r} is listed in env_vars but is not set"
            )
    return env


def describe_failure(message: str, exit_code: int | None) -> str:
    if exit_code == _SIGKILL_EXIT_CODE:
        return (
            f"{message} The container was killed (exit code 137). The usual cause "
            f"is that the run used more memory than its instance has; set a larger "
            f"instance with the {INSTANCE_TAG!r} run tag or the 'instance' config."
        )
    return message


class CloudflareContainerRunLauncher(RunLauncher, ConfigurableClass):
    """Launches each run in its own Cloudflare Container through a dispatcher Worker.

    Example ``dagster.yaml``:

    .. code-block:: yaml

        run_launcher:
          module: dagster_cloudflare
          class: CloudflareContainerRunLauncher
          config:
            endpoint: https://dagster-runs.example.workers.dev
            token:
              env: DAGSTER_CLOUDFLARE_TOKEN
            instance: standard-2
            env_vars:
              - DAGSTER_POSTGRES_PASSWORD

    A run can override the instance and image with the
    ``dagster-cloudflare/instance`` and ``dagster-cloudflare/image`` tags.
    """

    def __init__(
        self,
        endpoint: str,
        token: str,
        image: str | None = None,
        instance: InstanceSpec | None = None,
        env_vars: Sequence[str] | None = None,
        request_timeout: int = 30,
        inst_data: ConfigurableClassData | None = None,
    ):
        self._inst_data = check.opt_inst_param(
            inst_data, "inst_data", ConfigurableClassData
        )
        endpoint = check.str_param(endpoint, "endpoint")
        parts = urlsplit(endpoint)
        if parts.scheme != "https" and parts.hostname not in _LOOPBACK_HOSTS:
            raise check.CheckError(
                f"endpoint must use https, because requests carry the token and "
                f"run environment: {endpoint!r}"
            )
        self._endpoint = endpoint.rstrip("/")
        self._token = check.str_param(token, "token")
        self._image = check.opt_str_param(image, "image")
        self._instance_spec = _parse_instance(instance)
        self._env_vars = list(
            check.opt_sequence_param(env_vars, "env_vars", of_type=str)
        )
        self._request_timeout = check.int_param(request_timeout, "request_timeout")
        super().__init__()

    @property
    def inst_data(self) -> ConfigurableClassData | None:
        return self._inst_data

    @classmethod
    def config_type(cls):
        return {
            "endpoint": Field(
                StringSource,
                description="HTTPS URL of the dispatcher Worker.",
            ),
            "token": Field(
                StringSource,
                description="Bearer token that the Worker accepts (its DISPATCH_TOKEN secret).",
            ),
            "image": Field(
                Noneable(StringSource),
                is_required=False,
                default_value=None,
                description=(
                    "Name of the image in the Worker's container configuration. "
                    "The Worker uses its default image when this is not set."
                ),
            ),
            "instance": Field(
                ScalarUnion(
                    scalar_type=str,
                    non_scalar_schema={
                        "vcpu": Field(float),
                        "memoryMib": Field(int),
                        "diskMb": Field(int),
                    },
                ),
                is_required=False,
                description=(
                    "Instance type name such as 'standard-2', or a custom size "
                    "{vcpu, memoryMib, diskMb}. The Worker default applies when not set."
                ),
            ),
            "env_vars": Field(
                [StringSource],
                is_required=False,
                description=(
                    "Environment variables for the run container. 'KEY' copies the "
                    "value from the launcher process; 'KEY=VALUE' sets it."
                ),
            ),
            "request_timeout": Field(
                IntSource,
                is_required=False,
                default_value=30,
                description="Seconds to wait for each request to the Worker.",
            ),
        }

    @classmethod
    def from_config_value(
        cls, inst_data: ConfigurableClassData, config_value: Mapping[str, Any]
    ) -> "CloudflareContainerRunLauncher":
        return cls(inst_data=inst_data, **config_value)

    def _request(
        self, method: str, run_id: str, payload: Mapping[str, Any] | None = None
    ) -> dict[str, Any]:
        request = urllib.request.Request(
            f"{self._endpoint}/runs/{quote(run_id, safe='')}",
            data=None if payload is None else json.dumps(payload).encode(),
            headers={
                "Authorization": f"Bearer {self._token}",
                "Content-Type": "application/json",
                "User-Agent": "dagster-cloudflare",
            },
            method=method,
        )
        try:
            with urllib.request.urlopen(
                request, timeout=self._request_timeout
            ) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode(errors="replace")[:500]
            raise CloudflareWorkerError(
                f"Worker answered {method} with HTTP {exc.code}: {body}"
            ) from exc

    def _run_payload(self, run: DagsterRun) -> dict[str, Any]:
        origin = run.job_code_origin
        if origin is None:
            raise check.CheckError(f"Run {run.run_id} has no job code origin")
        command = ExecuteRunArgs(
            job_origin=origin,
            run_id=run.run_id,
            instance_ref=self._instance.get_ref(),
            set_exit_code_on_failure=True,
        ).get_command_args()
        instance = _parse_instance(run.tags.get(INSTANCE_TAG)) or self._instance_spec
        payload: dict[str, Any] = {
            "command": list(command),
            "env": _env_from_names(self._env_vars),
        }
        image = run.tags.get(IMAGE_TAG) or self._image
        if image:
            payload["image"] = image
        if instance:
            payload["instance"] = instance
        return payload

    def launch_run(self, context: LaunchRunContext) -> None:
        run = context.dagster_run
        payload = self._run_payload(run)
        try:
            result = self._request("POST", run.run_id, payload)
        except _TRANSPORT_ERRORS as exc:
            # The Worker may have started the container and lost the response.
            # The run ID is the Durable Object key, so ask before failing.
            try:
                result = self._request("GET", run.run_id)
            except (*_TRANSPORT_ERRORS, CloudflareWorkerError):
                raise exc
            if result.get("state") not in (*_ACTIVE_STATES, "success"):
                raise
        state = result.get("state")
        if state not in (*_ACTIVE_STATES, "success"):
            raise CloudflareWorkerError(
                f"Worker did not start run {run.run_id}: state {state!r}, "
                f"{result.get('message', '')}"
            )
        metadata = {"Worker state": state}
        if "instance" in payload:
            metadata["Instance"] = json.dumps(payload["instance"])
        if "image" in payload:
            metadata["Image"] = payload["image"]
        self._instance.report_engine_event(
            "Started Cloudflare Container for the run.",
            run,
            EngineEventData(metadata),
            cls=self.__class__,
        )

    def terminate(self, run_id: str) -> bool:
        run = self._instance.get_run_by_id(run_id)
        if not run or run.is_finished:
            return False
        self._instance.report_run_canceling(run)
        try:
            terminated = bool(self._request("DELETE", run_id).get("terminated"))
        except (*_TRANSPORT_ERRORS, CloudflareWorkerError) as exc:
            self._instance.report_engine_event(
                f"Could not stop the Cloudflare Container: {exc}",
                run,
                cls=self.__class__,
            )
            return False
        if not terminated:
            self._instance.report_engine_event(
                "The Worker did not stop the Cloudflare Container; it was not running.",
                run,
                cls=self.__class__,
            )
        return terminated

    @property
    def supports_check_run_worker_health(self) -> bool:
        return True

    def check_run_worker_health(self, run: DagsterRun) -> CheckRunHealthResult:
        try:
            result = self._request("GET", run.run_id)
        except (*_TRANSPORT_ERRORS, CloudflareWorkerError) as exc:
            return CheckRunHealthResult(
                WorkerStatus.UNKNOWN,
                msg=f"Could not read the run state from the Worker: {exc}",
                transient=True,
            )
        state = result.get("state")
        status = _STATUS_BY_STATE.get(state, WorkerStatus.UNKNOWN)
        message = result.get("message") or f"Worker state is {state!r}."
        if status == WorkerStatus.FAILED:
            message = describe_failure(message, result.get("exitCode"))
        return CheckRunHealthResult(status, msg=message)

    def get_run_worker_debug_info(
        self, run: DagsterRun, include_container_logs: bool | None = True
    ) -> str | None:
        try:
            result = self._request("GET", run.run_id)
        except (*_TRANSPORT_ERRORS, CloudflareWorkerError) as exc:
            return f"Could not read the run state from the Worker: {exc}"
        return json.dumps(result)


__all__ = [
    "IMAGE_TAG",
    "INSTANCE_TAG",
    "CloudflareContainerRunLauncher",
    "CloudflareWorkerError",
]

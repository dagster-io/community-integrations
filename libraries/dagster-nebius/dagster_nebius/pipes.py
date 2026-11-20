import hashlib
import json
import time
from collections.abc import Callable
from typing import Any, TypedDict
from uuid import uuid4

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    OpExecutionContext,
    PipesClient,
    PipesEnvContextInjector,
    open_pipes_session,
)
from dagster._core.definitions.resource_annotation import TreatAsResourceParam
from dagster._core.pipes.client import PipesClientCompletedInvocation
from nebius.aio.operation import Operation
from nebius.api.nebius.ai.v1 import (
    CancelJobRequest,
    CreateJobRequest,
    GetJobRequest,
    Job,
    JobServiceClient,
    JobSpec,
    JobStatus,
)
from nebius.api.nebius.common.v1 import ResourceMetadata
from nebius.sdk import SDK

from dagster_nebius._messages import _S3MessageReader
from dagster_nebius._utils import Deadline, failure, positive


class _Timeouts(TypedDict):
    timeout: float
    auth_timeout: float


_TERMINAL = {
    JobStatus.State.COMPLETED,
    JobStatus.State.FAILED,
    JobStatus.State.CANCELLED,
    JobStatus.State.ERROR,
}
_ACTIVE = {
    JobStatus.State.PROVISIONING,
    JobStatus.State.STARTING,
    JobStatus.State.IMAGE_PULLING,
    JobStatus.State.RUNNING,
    JobStatus.State.CANCELLING,
}


class PipesNebiusClient(PipesClient, TreatAsResourceParam):
    """Run one instrumented Nebius Serverless Job per Dagster invocation.

    Factories are evaluated inside run, after worker processes fork. They must
    return new SDK/S3 clients; this resource owns and closes those clients.
    Automatic retries are disabled pending reconciliation of any prior Job.
    """

    def __init__(
        self,
        *,
        sdk_factory: Callable[[], SDK],
        s3_client_factory: Callable[[], Any],
        bucket: str,
        key_prefix: str = "dagster-pipes",
        poll_interval: float = 5,
        rpc_timeout: float = 30,
        drain_timeout: float = 60,
        cancel_timeout: float = 120,
        forward_termination: bool = True,
    ):
        if not bucket or not key_prefix.strip("/"):
            raise ValueError("bucket and key_prefix must be nonempty")
        self.sdk_factory = sdk_factory
        self.s3_client_factory = s3_client_factory
        self.bucket = bucket
        self.key_prefix = key_prefix.strip("/")
        self.poll_interval = positive(poll_interval, "poll_interval")
        self.rpc_timeout = positive(rpc_timeout, "rpc_timeout")
        self.drain_timeout = positive(drain_timeout, "drain_timeout")
        self.cancel_timeout = positive(cancel_timeout, "cancel_timeout")
        self.forward_termination = forward_termination

    def run(
        self,
        *,
        context: AssetExecutionContext | OpExecutionContext,
        job_spec: JobSpec,
        project_id: str,
        wait_timeout: float,
        extras: dict[str, Any] | None = None,
    ) -> PipesClientCompletedInvocation:
        """Submit, supervise, and return explicit remote results.

        job_spec must have a positive provider timeout and restart_attempts=0.
        wait_timeout also covers provisioning. Storage objects and Job records
        are retained. This method never downloads or deletes workload outputs.
        """
        self._validate(job_spec, project_id)
        deadline = Deadline(wait_timeout)
        op_context = (
            context.op_execution_context
            if isinstance(context, AssetExecutionContext)
            else context
        )
        if op_context.retry_number:
            raise failure(
                "Reconcile the previous Nebius Job before retrying in a new Dagster run"
            )
        invocation = str(uuid4())
        prefix = f"{self.key_prefix}/{context.run.run_id}/{invocation}"
        name = f"dagster-{invocation}"
        sdk = self.sdk_factory()
        s3 = None
        job_id = ""
        operation_id = ""
        terminal = False
        operation: Operation | None = None
        try:
            s3 = self.s3_client_factory()
            reader = _S3MessageReader(s3, self.bucket, prefix)
            service = JobServiceClient(sdk)
            with open_pipes_session(
                context=context,
                context_injector=PipesEnvContextInjector(),
                message_reader=reader,
                extras=extras,
            ) as session:
                # Generated SDK messages require their copy constructor, not deepcopy.
                spec = JobSpec(job_spec)
                spec.environment_variables = [
                    *spec.environment_variables,
                    *[
                        JobSpec.EnvironmentVariable(name=key, value=value)
                        for key, value in session.get_bootstrap_env_vars().items()
                    ],
                ]
                request = CreateJobRequest(
                    metadata=ResourceMetadata(parent_id=project_id, name=name),
                    spec=spec,
                )
                # Keep the same non-secret receipt in logs and failure metadata so
                # an uncertain submission can be reconciled from the Dagster event.
                receipt = {
                    "job_name": name,
                    "project_id": project_id,
                    "idempotency_key": invocation,
                    "pipes_uri": f"s3://{self.bucket}/{prefix}",
                    "spec_sha256": hashlib.sha256(spec.SerializeToString()).hexdigest(),
                }
                context.log.info("Nebius submission receipt: %s", json.dumps(receipt))
                try:
                    operation = service.create(
                        request,
                        metadata=[("x-idempotency-key", invocation)],
                        retries=0,
                        **self._timeouts(deadline),
                    ).wait()
                except Exception:
                    raise failure(
                        "Nebius submission failed or acknowledgement was lost. "
                        "Reconcile all Jobs matching the receipt's project/name; "
                        "names are not unique. Inspect their specs and operation IDs "
                        "before cancellation or resubmission.",
                        **receipt,
                    ) from None
                job_id = operation.resource_id
                operation_id = operation.id
                context.log.info(
                    "Nebius operation_id=%s job_id=%s", operation_id, job_id
                )
                self._wait_operation(operation, deadline, reader)
                job_id = operation.resource_id
                if not job_id:
                    raise failure(
                        "Nebius create operation returned no Job ID",
                        operation_id=operation_id,
                    )
                while True:
                    job = service.get(
                        GetJobRequest(id=job_id), retries=2, **self._timeouts(deadline)
                    ).wait()
                    terminal = job.status.state in _TERMINAL
                    if terminal:
                        break
                    if job.status.state not in _ACTIVE:
                        raise failure(
                            "Unknown or externally deleting Nebius Job state",
                            job_id=job_id,
                            state=str(job.status.state),
                        )
                    reader.poll(deadline)
                    time.sleep(min(self.poll_interval, deadline.remaining()))
                if job.status.state != JobStatus.State.COMPLETED:
                    raise failure(
                        "Nebius Job did not complete successfully; inspect provider logs and state details",
                        job_id=job_id,
                        state=str(job.status.state),
                        state_code=job.status.state_details.code,
                    )
                drain = Deadline(min(self.drain_timeout, deadline.remaining()))
                while not reader.closed:
                    reader.poll(drain)
                    if not reader.closed:
                        time.sleep(min(self.poll_interval, drain.remaining()))
                results = session.get_reported_results()
                if context.has_assets_def:
                    reported = {
                        r.asset_key for r in results if isinstance(r, MaterializeResult)
                    }
                    if reported != set(context.selected_asset_keys):
                        raise failure(
                            "Missing or unexpected explicit Pipes materializations",
                            job_id=job_id,
                        )
            return PipesClientCompletedInvocation(
                session, metadata=self._metadata(job, operation_id, prefix)
            )
        except BaseException as exc:
            if operation is not None:
                job_id = operation.resource_id or job_id
            if job_id and not terminal and self.forward_termination:
                try:
                    self._cancel(JobServiceClient(sdk), job_id)
                    context.log.info(
                        "Nebius Job reached a terminal state after cancellation: %s",
                        job_id,
                    )
                except BaseException:
                    context.log.error(
                        "Nebius cancellation UNCONFIRMED; reconcile job_id=%s", job_id
                    )
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            # Dagster interruptions inherit BaseException and must retain their identity.
            if not isinstance(exc, Exception):
                raise
            from dagster import Failure

            if isinstance(exc, Failure):
                raise
            raise failure(
                "Nebius invocation failed; inspect its receipt and provider logs",
                job_id=job_id,
                operation_id=operation_id,
                error_type=type(exc).__name__,
            ) from None
        finally:
            # Cleanup errors should not replace an already observed workload outcome.
            if s3 is not None:
                try:
                    s3.close()
                except Exception:
                    context.log.warning("Could not close the S3 client")
            try:
                sdk.sync_close(timeout=self.rpc_timeout)
            except Exception:
                context.log.warning("Could not close the Nebius SDK")

    def _timeouts(self, deadline: Deadline) -> _Timeouts:
        remaining = min(self.rpc_timeout, deadline.remaining())
        return {"timeout": remaining, "auth_timeout": remaining}

    def _wait_operation(
        self,
        operation: Operation,
        deadline: Deadline,
        reader: _S3MessageReader | None = None,
    ) -> None:
        while not operation.done():
            if reader is not None:
                reader.poll(deadline)
            operation.sync_update(retries=2, **self._timeouts(deadline))
            if not operation.done():
                time.sleep(min(self.poll_interval, deadline.remaining()))
        if not operation.successful():
            raise failure(
                "Nebius lifecycle operation failed", operation_id=operation.id
            )

    def _cancel(self, service: JobServiceClient, job_id: str) -> None:
        deadline = Deadline(self.cancel_timeout)
        job = service.get(
            GetJobRequest(id=job_id), retries=2, **self._timeouts(deadline)
        ).wait()
        if job.status.state in _TERMINAL:
            return
        operation = service.cancel(
            CancelJobRequest(id=job_id), retries=0, **self._timeouts(deadline)
        ).wait()
        self._wait_operation(operation, deadline)
        while True:
            job = service.get(
                GetJobRequest(id=job_id), retries=2, **self._timeouts(deadline)
            ).wait()
            if job.status.state in _TERMINAL:
                return
            time.sleep(min(self.poll_interval, deadline.remaining()))

    @staticmethod
    def _validate(spec: JobSpec, project_id: str) -> None:
        if not project_id or not all(
            [spec.image, spec.platform, spec.preset, spec.subnet_id]
        ):
            raise ValueError(
                "project_id, image, platform, preset and subnet_id are required"
            )
        if spec.disk.size_bytes <= 0 or not spec.disk.type:
            raise ValueError("A positive disk size and disk type are required")
        if spec.timeout is None or spec.timeout.total_seconds() <= 0:
            raise ValueError("A positive provider Job timeout is required")
        if spec.restart_attempts != 0:
            raise ValueError(
                "Provider restarts are not supported; set restart_attempts=0"
            )
        if any(
            env.name.startswith("DAGSTER_PIPES_") for env in spec.environment_variables
        ):
            raise ValueError("DAGSTER_PIPES_* environment variables are reserved")

    def _metadata(self, job: Job, operation_id: str, prefix: str) -> dict[str, Any]:
        return {
            "nebius/job_id": job.metadata.id,
            "nebius/operation_id": operation_id,
            "nebius/project_id": job.metadata.parent_id,
            "nebius/platform": job.spec.platform,
            "nebius/preset": job.spec.preset,
            "nebius/image": job.spec.image,
            "nebius/state": "COMPLETED",
            "nebius/pipes_uri": f"s3://{self.bucket}/{prefix}",
            "nebius/started_at": job.status.started_at.isoformat()
            if job.status.started_at
            else None,
            "nebius/finished_at": job.status.finished_at.isoformat()
            if job.status.finished_at
            else None,
        }

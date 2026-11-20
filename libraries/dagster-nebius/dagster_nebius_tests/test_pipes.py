import hashlib
import json
from types import SimpleNamespace

import dagster as dg
import pytest
from dagster_pipes import decode_param
from nebius.api.nebius.ai.v1 import Job, JobSpec, JobStatus
from nebius.api.nebius.common.v1 import ResourceMetadata

from dagster_nebius import PipesNebiusClient


def message(method, params):
    return {"__dagster_pipes_version": "0.1", "method": method, "params": params}


def events(key="gpu_result"):
    return [
        message("opened", {"extras": {}}),
        message("log", {"message": "GPU workload log", "level": "INFO"}),
        message(
            "report_asset_materialization",
            {
                "asset_key": key,
                "data_version": "model-v1",
                "metadata": {"rows": {"raw_value": 3, "type": "int"}},
            },
        ),
        message("closed", {}),
    ]


class FakeOperation:
    id = "operation-test"
    resource_id = "job-test"

    def done(self):
        return True

    def successful(self):
        return True


class FakeService:
    def __init__(self, s3, state=JobStatus.State.COMPLETED, messages=None):
        self.s3 = s3
        self.state = state
        self.messages = events() if messages is None else messages
        self.creates = []
        self.cancelled = []
        self.get_error = None
        self.create_error = None
        self.wait_error = None
        self.operation = FakeOperation()

    def create(self, request, **kwargs):
        self.creates.append((request, kwargs))
        if self.create_error:
            raise self.create_error
        self.request = request
        env = {e.name: e.value for e in request.spec.environment_variables}
        self.params = decode_param(env["DAGSTER_PIPES_MESSAGES"])
        if self.messages:
            self.s3.put_object(
                Bucket=self.params["bucket"],
                Key=f"{self.params['key_prefix']}/1.json",
                Body="\n".join(json.dumps(m) for m in self.messages),
            )

        def wait():
            if self.wait_error:
                raise self.wait_error
            return self.operation

        return SimpleNamespace(wait=wait)

    def get(self, request, **kwargs):
        if self.get_error:
            error, self.get_error = self.get_error, None
            raise error
        return SimpleNamespace(
            wait=lambda: Job(
                metadata=ResourceMetadata(id="job-test", parent_id="project-test"),
                spec=self.request.spec,
                status=JobStatus(state=self.state),
            )
        )

    def cancel(self, request, **kwargs):
        self.cancelled.append(request.id)
        self.state = JobStatus.State.CANCELLED
        return SimpleNamespace(wait=FakeOperation)


def client_for(monkeypatch, sdk, s3, service, **kwargs):
    monkeypatch.setattr("dagster_nebius.pipes.JobServiceClient", lambda _: service)
    return PipesNebiusClient(
        sdk_factory=lambda: sdk,
        s3_client_factory=lambda: s3,
        bucket="pipes-test",
        poll_interval=0.001,
        drain_timeout=0.05,
        cancel_timeout=0.03,
        **kwargs,
    )


def execute(client, spec, wait_timeout=2, **kwargs):
    @dg.asset
    def gpu_result(context: dg.AssetExecutionContext):
        return client.run(
            context=context,
            job_spec=spec,
            project_id="project-test",
            wait_timeout=wait_timeout,
            **kwargs,
        ).get_materialize_result(implicit_materialization=False)

    return dg.materialize([gpu_result], raise_on_error=False)


def test_success_and_request_copy(monkeypatch, sdk, s3, spec):
    service = FakeService(s3)
    client = client_for(monkeypatch, sdk, s3, service)
    result = execute(client, spec, extras={"input_uri": "s3://data/in"})
    assert result.success
    event = result.asset_materializations_for_node("gpu_result")[0]
    assert event.metadata["rows"].value == 3
    assert event.metadata["nebius/job_id"].value == "job-test"
    assert event.tags["dagster/data_version"] == "model-v1"
    request, kwargs = service.creates[0]
    assert request.spec.image == spec.image
    assert len(spec.environment_variables) == 0
    assert kwargs["retries"] == 0
    assert kwargs["metadata"][0][0] == "x-idempotency-key"
    assert len(service.creates) == 1
    assert not service.cancelled
    sdk.sync_close.assert_called_once()


@pytest.mark.parametrize(
    "state", [JobStatus.State.FAILED, JobStatus.State.ERROR, JobStatus.State.CANCELLED]
)
def test_terminal_failure_no_materialization(monkeypatch, sdk, s3, spec, state):
    service = FakeService(s3, state)
    result = execute(client_for(monkeypatch, sdk, s3, service), spec)
    assert not result.success
    assert not result.asset_materializations_for_node("gpu_result")
    assert not service.cancelled


@pytest.mark.parametrize(
    "messages",
    [
        [],
        events()[:-1],
        events("wrong_asset"),
        [message("opened", {}), message("closed", {})],
    ],
)
def test_missing_or_wrong_protocol_fails(monkeypatch, sdk, s3, spec, messages):
    service = FakeService(s3, messages=messages)
    result = execute(client_for(monkeypatch, sdk, s3, service), spec)
    assert not result.success
    assert not result.asset_materializations_for_node("gpu_result")


@pytest.mark.parametrize("failure_phase", ["create", "wait"])
def test_lost_create_ack_retains_receipt_without_retry(
    monkeypatch, sdk, s3, spec, failure_phase, capsys
):
    service = FakeService(s3)
    setattr(service, f"{failure_phase}_error", RuntimeError("SECRET_SENTINEL"))
    spec.environment_variables = [
        JobSpec.EnvironmentVariable(name="TOKEN", value="REQUEST_SECRET")
    ]
    result = execute(client_for(monkeypatch, sdk, s3, service), spec)
    assert not result.success
    assert not result.asset_materializations_for_node("gpu_result")
    assert len(service.creates) == 1
    # No acknowledged ID; a name cannot identify ownership.
    assert not service.cancelled
    request, kwargs = service.creates[0]
    failure_data = result.failure_data_for_node("gpu_result")
    metadata = {
        key: value.value
        for key, value in failure_data.user_failure_data.metadata.items()
    }
    env = {e.name: e.value for e in request.spec.environment_variables}
    params = decode_param(env["DAGSTER_PIPES_MESSAGES"])
    assert metadata == {
        "job_name": request.metadata.name,
        "project_id": "project-test",
        "idempotency_key": kwargs["metadata"][0][1],
        "pipes_uri": f"s3://{params['bucket']}/{params['key_prefix']}",
        "spec_sha256": hashlib.sha256(request.spec.SerializeToString()).hexdigest(),
    }
    assert kwargs["retries"] == 0
    error = failure_data.error.to_string()
    assert "Reconcile all Jobs" in error
    assert "names are not unique" in error
    output = capsys.readouterr().err
    assert json.dumps(metadata) in output
    for secret in ("SECRET_SENTINEL", "REQUEST_SECRET"):
        assert secret not in error + output + json.dumps(metadata)
    sdk.sync_close.assert_called_once()


def test_interrupt_cancels_owned_job(monkeypatch, sdk, s3, spec):
    service = FakeService(s3, JobStatus.State.RUNNING)
    service.get_error = dg.DagsterExecutionInterruptedError()
    result = execute(client_for(monkeypatch, sdk, s3, service), spec)
    assert not result.success
    assert service.cancelled == ["job-test"]
    assert not result.asset_materializations_for_node("gpu_result")


def test_timeout_cancels_owned_job(monkeypatch, sdk, s3, spec):
    service = FakeService(s3, JobStatus.State.RUNNING)
    result = execute(client_for(monkeypatch, sdk, s3, service), spec, wait_timeout=0.02)
    assert not result.success
    assert service.cancelled == ["job-test"]


def test_cancellation_failure_preserves_error(monkeypatch, sdk, s3, spec, capsys):
    service = FakeService(s3, JobStatus.State.RUNNING)
    service.get_error = ValueError("ORIGINAL_SECRET")
    service.cancel = lambda *a, **k: (_ for _ in ()).throw(
        RuntimeError("CANCEL_SECRET")
    )
    result = execute(client_for(monkeypatch, sdk, s3, service), spec)
    assert not result.success
    output = capsys.readouterr().err
    assert "UNCONFIRMED" in output
    assert "CANCEL_SECRET" not in output
    assert "ORIGINAL_SECRET" not in output


def test_no_forward_termination(monkeypatch, sdk, s3, spec):
    service = FakeService(s3, JobStatus.State.RUNNING)
    service.get_error = dg.DagsterExecutionInterruptedError()
    result = execute(
        client_for(monkeypatch, sdk, s3, service, forward_termination=False), spec
    )
    assert not result.success
    assert not service.cancelled


@pytest.mark.parametrize(
    "field,value", [("restart_attempts", -1), ("timeout", None), ("image", "")]
)
def test_invalid_spec_never_submits(monkeypatch, sdk, s3, spec, field, value):
    setattr(spec, field, value)
    service = FakeService(s3)
    assert not execute(client_for(monkeypatch, sdk, s3, service), spec).success
    assert not service.creates
    sdk.sync_close.assert_not_called()


def test_reserved_environment_rejected(monkeypatch, sdk, s3, spec):
    from nebius.api.nebius.ai.v1 import JobSpec

    spec.environment_variables = [
        JobSpec.EnvironmentVariable(name="DAGSTER_PIPES_CONTEXT", value="bad")
    ]
    service = FakeService(s3)
    assert not execute(client_for(monkeypatch, sdk, s3, service), spec).success
    assert not service.creates


def test_operation_polled_with_bounded_requests(monkeypatch, sdk, s3, spec):
    service = FakeService(s3)

    class PendingOperation(FakeOperation):
        count = 0

        def done(self):
            return self.count == 2

        def sync_update(self, **kwargs):
            assert 0 < kwargs["timeout"] <= 30
            assert 0 < kwargs["auth_timeout"] <= 30
            self.count += 1

    service.operation = PendingOperation()
    assert execute(client_for(monkeypatch, sdk, s3, service), spec).success
    assert service.operation.count == 2


def test_failed_create_operation_is_not_job_success(monkeypatch, sdk, s3, spec):
    service = FakeService(s3)
    service.operation.successful = lambda: False
    result = execute(client_for(monkeypatch, sdk, s3, service), spec)
    assert not result.success
    assert not result.asset_materializations_for_node("gpu_result")


def test_protocol_failure_while_running_cancels(monkeypatch, sdk, s3, spec):
    service = FakeService(s3, JobStatus.State.RUNNING, [message("closed", {})])
    result = execute(client_for(monkeypatch, sdk, s3, service), spec)
    assert not result.success
    assert service.cancelled == ["job-test"]


def test_registered_resource_factories_deferred(monkeypatch, sdk, s3, spec):
    from unittest.mock import Mock

    service = FakeService(s3)
    client = client_for(monkeypatch, sdk, s3, service)
    client.sdk_factory = Mock(return_value=sdk)

    @dg.asset
    def gpu_result(context: dg.AssetExecutionContext, nebius: PipesNebiusClient):
        return nebius.run(
            context=context, job_spec=spec, project_id="project-test", wait_timeout=2
        ).get_materialize_result(implicit_materialization=False)

    defs = dg.Definitions(assets=[gpu_result], resources={"nebius": client})
    client.sdk_factory.assert_not_called()
    assert dg.materialize([gpu_result], resources=defs.resources).success
    client.sdk_factory.assert_called_once()


def test_two_runs_have_separate_protocol_prefixes(monkeypatch, sdk, s3, spec):
    service = FakeService(s3)
    client = client_for(monkeypatch, sdk, s3, service)
    assert execute(client, spec).success
    first = service.params["key_prefix"]
    assert execute(client, spec).success
    assert first != service.params["key_prefix"]


def test_automatic_retry_does_not_resubmit(monkeypatch, sdk, s3, spec):
    service = FakeService(s3)
    service.create_error = TimeoutError()
    client = client_for(monkeypatch, sdk, s3, service)

    @dg.asset(retry_policy=dg.RetryPolicy(max_retries=2))
    def gpu_result(context: dg.AssetExecutionContext):
        return client.run(
            context=context, job_spec=spec, project_id="project-test", wait_timeout=1
        ).get_materialize_result(implicit_materialization=False)

    result = dg.materialize([gpu_result], raise_on_error=False)
    assert not result.success
    assert len(service.creates) == 1

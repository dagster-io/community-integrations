import json
import time

import pytest
from dagster import reconstructable
from dagster._check import CheckError
from dagster._core.launcher.base import LaunchRunContext, WorkerStatus
from dagster._core.storage.dagster_run import DagsterRunStatus
from dagster._core.test_utils import instance_for_test

from dagster_cloudflare import CloudflareContainerRunLauncher
from dagster_cloudflare.containers.run_launcher import INSTANCE_TAG
from dagster_cloudflare_tests.fake_worker import TOKEN, FakeRun, FakeWorker
from dagster_cloudflare_tests.jobs import env_job, slow_job


@pytest.fixture
def worker():
    fake = FakeWorker().start()
    yield fake
    fake.stop()


def launcher_config(worker, **extra):
    return {
        "module": "dagster_cloudflare",
        "class": "CloudflareContainerRunLauncher",
        "config": {"endpoint": worker.endpoint, "token": TOKEN, **extra},
    }


@pytest.fixture
def instance(worker, monkeypatch):
    monkeypatch.setenv("FROM_LAUNCHER", "yes")
    with instance_for_test(
        overrides={
            "run_launcher": launcher_config(
                worker, env_vars=["FROM_LAUNCHER", "SET_HERE=1"], instance="standard-1"
            )
        }
    ) as inst:
        yield inst


def create_run(instance, job_def, tags=None):
    recon = reconstructable(job_def)
    return instance.create_run_for_job(
        job_def=job_def,
        job_code_origin=recon.get_python_origin(),
        tags=tags or {},
    )


def launch(instance, run):
    instance.run_launcher.launch_run(LaunchRunContext(dagster_run=run, workspace=None))


def wait_for(instance, run_id, statuses, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        run = instance.get_run_by_id(run_id)
        if run.status in statuses:
            return run
        time.sleep(0.5)
    raise AssertionError(f"run {run_id} still {instance.get_run_by_id(run_id).status}")


def test_launched_run_executes_with_env_and_instance(instance, worker):
    run = create_run(
        instance, env_job, tags={INSTANCE_TAG: '{"vcpu": 2, "memoryMib": 8192}'}
    )
    launch(instance, run)

    finished = wait_for(
        instance, run.run_id, {DagsterRunStatus.SUCCESS, DagsterRunStatus.FAILURE}
    )
    assert finished.status == DagsterRunStatus.SUCCESS

    payload = worker.runs[run.run_id].payload
    assert payload["command"][-3:-1] == ["api", "execute_run"]
    assert payload["env"] == {"FROM_LAUNCHER": "yes", "SET_HERE": "1"}
    assert payload["instance"] == {"vcpu": 2, "memoryMib": 8192}  # tag beats config
    assert "image" not in payload

    # Dagster records SUCCESS just before the process exits.
    worker.runs[run.run_id].process.wait(timeout=30)
    health = instance.run_launcher.check_run_worker_health(finished)
    assert health.status == WorkerStatus.SUCCESS


def test_config_instance_used_without_tag(instance, worker):
    worker.execute = False
    run = create_run(instance, env_job)
    launch(instance, run)
    assert worker.runs[run.run_id].payload["instance"] == "standard-1"


def test_terminate_cancels_a_running_run(instance, worker):
    run = create_run(instance, slow_job)
    launch(instance, run)
    wait_for(instance, run.run_id, {DagsterRunStatus.STARTED})

    assert instance.run_launcher.terminate(run.run_id)
    canceled = wait_for(instance, run.run_id, {DagsterRunStatus.CANCELED})
    assert canceled.status == DagsterRunStatus.CANCELED
    assert ("DELETE", f"/runs/{run.run_id}") in worker.requests
    # A finished run is not terminated again.
    assert not instance.run_launcher.terminate(run.run_id)


def test_lost_launch_response_is_confirmed_with_get(instance, worker):
    worker.execute = False
    worker.drop_next_post_response = True
    run = create_run(instance, env_job)
    launch(instance, run)
    assert [m for m, _ in worker.requests] == ["POST", "GET"]


def test_launch_fails_when_worker_rejects(instance, worker):
    worker.execute = False
    run = create_run(instance, env_job)
    worker.runs[run.run_id] = FakeRun(
        payload={}, forced_state={"state": "failed", "message": "no capacity"}
    )
    with pytest.raises(Exception, match="no capacity"):
        launch(instance, run)


def test_health_check_explains_out_of_memory(instance, worker):
    worker.execute = False
    run = create_run(instance, env_job)
    worker.runs[run.run_id] = FakeRun(
        payload={},
        forced_state={
            "state": "failed",
            "message": "Container exited with code 137.",
            "exitCode": 137,
        },
    )
    health = instance.run_launcher.check_run_worker_health(run)
    assert health.status == WorkerStatus.FAILED
    assert "more memory than its instance" in health.msg
    assert INSTANCE_TAG in health.msg


def test_health_check_maps_states(instance, worker):
    run = create_run(instance, env_job)
    worker.execute = False
    for state, status in [
        ("starting", WorkerStatus.RUNNING),
        ("running", WorkerStatus.RUNNING),
        ("canceled", WorkerStatus.FAILED),
        ("weird", WorkerStatus.UNKNOWN),
    ]:
        worker.runs[run.run_id] = FakeRun(payload={}, forced_state={"state": state})
        assert instance.run_launcher.check_run_worker_health(run).status == status
    del worker.runs[run.run_id]
    assert (
        instance.run_launcher.check_run_worker_health(run).status
        == WorkerStatus.NOT_FOUND
    )


def test_unreachable_worker_is_a_transient_health_result(instance, worker):
    run = create_run(instance, env_job)
    worker.stop()
    result = instance.run_launcher.check_run_worker_health(run)
    assert result.status == WorkerStatus.UNKNOWN
    assert result.transient


def test_wrong_token_is_reported(worker):
    launcher = CloudflareContainerRunLauncher(endpoint=worker.endpoint, token="wrong")
    with instance_for_test() as inst:
        launcher.register_instance(inst)
        run = create_run(inst, env_job)
        with pytest.raises(Exception, match="HTTP 401"):
            launcher.launch_run(LaunchRunContext(dagster_run=run, workspace=None))


def test_endpoint_must_be_https_unless_loopback():
    with pytest.raises(CheckError, match="https"):
        CloudflareContainerRunLauncher(endpoint="http://runs.example.com", token="t")
    CloudflareContainerRunLauncher(endpoint="https://runs.example.com", token="t")
    CloudflareContainerRunLauncher(endpoint="http://localhost:8787", token="t")


def test_missing_env_var_fails_launch(worker, monkeypatch):
    monkeypatch.delenv("NOT_SET_ANYWHERE", raising=False)
    with instance_for_test(
        overrides={
            "run_launcher": launcher_config(worker, env_vars=["NOT_SET_ANYWHERE"])
        }
    ) as inst:
        run = create_run(inst, env_job)
        with pytest.raises(CheckError, match="NOT_SET_ANYWHERE"):
            launch(inst, run)


def test_custom_instance_and_image_from_yaml_config(worker):
    worker.execute = False
    size = {"vcpu": 0.5, "memoryMib": 4096, "diskMb": 4000}
    with instance_for_test(
        overrides={"run_launcher": launcher_config(worker, instance=size, image="big")}
    ) as inst:
        run = create_run(inst, env_job, tags={"dagster-cloudflare/image": "bigger"})
        launch(inst, run)
        payload = worker.runs[run.run_id].payload
        assert payload["instance"] == size
        assert payload["image"] == "bigger"
        assert json.loads(json.dumps(payload))  # serializable as sent

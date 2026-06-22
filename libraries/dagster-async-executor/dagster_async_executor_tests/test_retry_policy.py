import os
import tempfile

import dagster as dg
import pytest

from dagster_async_executor import async_executor

_ATTEMPTS_FILE = os.path.join(tempfile.gettempdir(), "async_executor_retry_attempts")


def fail_once_then_succeed_job() -> dg.JobDefinition:
    @dg.op(retry_policy=dg.RetryPolicy(max_retries=1, delay=0.5))
    def fail_once_then_succeed() -> int:
        with open(_ATTEMPTS_FILE) as f:
            attempts = int(f.read() or "0")
        with open(_ATTEMPTS_FILE, "w") as f:
            f.write(str(attempts + 1))
        if attempts == 0:
            raise RuntimeError("intentional first-attempt failure")
        return attempts

    @dg.job(executor_def=async_executor)
    def retry_job():
        fail_once_then_succeed()

    return retry_job


@pytest.mark.timeout(30)
def test_delayed_retry_policy_does_not_hang():
    with open(_ATTEMPTS_FILE, "w") as f:
        f.write("0")
    with (
        dg.instance_for_test() as instance,
        dg.execute_job(
            dg.reconstructable(fail_once_then_succeed_job),
            instance=instance,
        ) as result,
    ):
        assert result.success
        # Prove the delayed retry actually ran (not a first-attempt success):
        # the op returns `attempts`, which is 1 only on the retried attempt.
        assert result.output_for_node("fail_once_then_succeed") == 1
    with open(_ATTEMPTS_FILE) as f:
        assert f.read() == "2"  # op compute ran exactly twice

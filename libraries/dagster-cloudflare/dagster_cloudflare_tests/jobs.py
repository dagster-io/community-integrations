import os
import time

from dagster import OpExecutionContext, job, op


@op
def check_env(context: OpExecutionContext) -> None:
    # The launcher passes env_vars to the container.
    assert os.environ["FROM_LAUNCHER"] == "yes", os.environ.get("FROM_LAUNCHER")
    context.log.info("env ok")


@job
def env_job():
    check_env()


@op
def sleep_long() -> None:
    time.sleep(120)


@job
def slow_job():
    sleep_long()

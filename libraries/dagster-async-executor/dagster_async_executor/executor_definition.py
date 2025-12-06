from typing import TYPE_CHECKING

from dagster._core.definitions.executor_definition import ExecutorConfig, executor
from dagster_shared import check
from dagster._builtins import Int
from dagster._config import Field, Noneable
from dagster._core.execution.retries import RetryMode, get_retries_config
from dagster._core.execution.step_dependency_config import (
    StepDependencyConfig,
    get_step_dependency_config_field,
)
from dagster._core.execution.tags import get_tag_concurrency_limits_config

if TYPE_CHECKING:
    from dagster_async_executor.executor import AsyncExecutor

ASYNC_CONFIG = Field(
    {
        "max_concurrent": Field(
            Noneable(Int),
            default_value=None,
            description=(
                "The number of asynchronous tasks that may run concurrently. "
                "By default, this is set to None, which allows for an unlimited number of "
                "concurrent tasks."
            ),
        ),
        "tag_concurrency_limits": get_tag_concurrency_limits_config(),
        "retries": get_retries_config(),
        "step_dependency_config": get_step_dependency_config_field(),
    },
)


@executor(
    name="async_executor",
    config_schema=ASYNC_CONFIG,
)
def async_executor(init_context):
    return _core_async_executor_creation(init_context.executor_config)


def _core_async_executor_creation(config: ExecutorConfig) -> "AsyncExecutor":
    from dagster_async_executor.executor import AsyncExecutor

    return AsyncExecutor(
        retries=RetryMode.from_config(check.dict_elem(config, "retries")),  # type: ignore  # (possible none)
        max_concurrent=check.opt_int_elem(config, "max_concurrent"),
        tag_concurrency_limits=check.opt_list_elem(config, "tag_concurrency_limits"),
        step_dependency_config=StepDependencyConfig.from_config(
            check.opt_nullable_dict_elem(config, "step_dependency_config")
        ),
    )

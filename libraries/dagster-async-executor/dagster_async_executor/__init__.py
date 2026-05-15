from dagster._core.libraries import DagsterLibraryRegistry

from dagster_async_executor.executor_definition import async_executor as async_executor

__version__ = "0.0.2"

DagsterLibraryRegistry.register(
    "dagster-async-executor", __version__, is_dagster_package=False
)

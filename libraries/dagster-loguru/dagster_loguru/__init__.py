from dagster._core.libraries import DagsterLibraryRegistry

from dagster_loguru.capture import capture_loguru_logs

__version__ = "0.0.1"

DagsterLibraryRegistry.register("dagster-loguru", __version__, is_dagster_package=False)

__all__ = ["capture_loguru_logs"]

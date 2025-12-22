from dagster_openlineage.version import __version__

from dagster_openlineage.adapter import OpenLineageAdapter
from dagster_openlineage.listener import OpenLineageEventListener
from dagster_openlineage.utils import make_step_job_name, to_utc_iso_8601
from dagster_openlineage.compat import (
    get_pipeline_origin,
    get_job_origin,
    get_repository_origin,
)

import importlib.util
import inspect

_dagster_lib_spec = importlib.util.find_spec("dagster._core.libraries")
if _dagster_lib_spec is not None:
    from dagster._core.libraries import DagsterLibraryRegistry

    _register_sig = inspect.signature(DagsterLibraryRegistry.register)
    _has_is_dagster_package = "is_dagster_package" in _register_sig.parameters

    if _has_is_dagster_package:
        DagsterLibraryRegistry.register(
            "dagster-openlineage", __version__, is_dagster_package=False
        )
    else:
        DagsterLibraryRegistry.register("dagster-openlineage", __version__)

__all__ = [
    "OpenLineageAdapter",
    "OpenLineageEventListener",
    "make_step_job_name",
    "to_utc_iso_8601",
    "get_pipeline_origin",
    "get_job_origin",
    "get_repository_origin",
]

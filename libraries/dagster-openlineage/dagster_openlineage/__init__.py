from dagster._core.libraries import DagsterLibraryRegistry

from dagster_openlineage.resource import OpenLineageResource
from dagster_openlineage.listener import OpenLineageEventListener

__version__ = "0.0.0"

DagsterLibraryRegistry.register("dagster-openlineage", __version__, is_dagster_package=False)


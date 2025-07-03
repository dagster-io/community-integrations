from dagster._core.libraries import DagsterLibraryRegistry

from dagster_teradata.resources import (
    TeradataDagsterConnection as TeradataDagsterConnection,
    TeradataResource as TeradataResource,
    fetch_last_updated_timestamps as fetch_last_updated_timestamps,
    teradata_resource as teradata_resource,
)

from dagster_teradata.teradata_compute_cluster_manager import (
    TeradataComputeClusterManager as TeradataComputeClusterManager,
)

from dagster_teradata.ttu.bteq import Bteq as Bteq

__version__ = "0.0.5"

DagsterLibraryRegistry.register(
    "dagster-teradata", __version__, is_dagster_package=False
)

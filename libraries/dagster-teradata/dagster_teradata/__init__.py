from dagster._core.libraries import DagsterLibraryRegistry

from dagster_teradata.resources import (
    TeradataDagsterConnection as TeradataDagsterConnection,
    TeradataResource as TeradataResource,
    fetch_last_updated_timestamps as fetch_last_updated_timestamps,
    teradata_resource as teradata_resource,
)

__version__ = "0.0.3"

DagsterLibraryRegistry.register(
    "dagster-teradata", __version__, is_dagster_package=False
)

from dagster._core.libraries import DagsterLibraryRegistry

__version__ = "0.0.6"

DagsterLibraryRegistry.register(
    "dagster-contrib-gcp", __version__, is_dagster_package=False
)

from dagster._core.libraries import DagsterLibraryRegistry

__version__ = "0.1.6"

DagsterLibraryRegistry.register(
    "dagster-hightouch", __version__, is_dagster_package=False
)

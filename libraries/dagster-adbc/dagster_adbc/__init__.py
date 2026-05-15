from dagster._core.libraries import DagsterLibraryRegistry

from dagster_adbc.resource import ADBCResource

__all__ = ["ADBCResource"]
__version__ = "0.0.1"

DagsterLibraryRegistry.register("dagster-adbc", __version__, is_dagster_package=False)

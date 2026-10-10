from dagster._core.libraries import DagsterLibraryRegistry

from dagster_adbc.io_manager import ADBCIOManager
from dagster_adbc.resource import ADBCResource

__all__ = ["ADBCIOManager", "ADBCResource"]
__version__ = "0.0.2"

DagsterLibraryRegistry.register("dagster-adbc", __version__, is_dagster_package=False)

from dagster._core.libraries import DagsterLibraryRegistry

from dagster_xquik.client import XquikClient, XquikError
from dagster_xquik.resources import XquikResource

__version__ = "0.0.1"

DagsterLibraryRegistry.register("dagster-xquik", __version__, is_dagster_package=False)

__all__ = ["XquikClient", "XquikError", "XquikResource"]

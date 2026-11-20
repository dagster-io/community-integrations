from dagster._core.libraries import DagsterLibraryRegistry

from dagster_pixeltable.resource import PixeltableResource

__version__ = "0.1.0"

__all__ = ["PixeltableResource"]

DagsterLibraryRegistry.register(
    "dagster-pixeltable", __version__, is_dagster_package=False
)

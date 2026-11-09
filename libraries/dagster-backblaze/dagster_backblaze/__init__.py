from dagster._core.libraries import DagsterLibraryRegistry

from dagster_backblaze.b2.resources import B2Resource

__version__ = "0.0.1"

DagsterLibraryRegistry.register(
    "dagster-backblaze", __version__, is_dagster_package=False
)

__all__ = ["B2Resource"]

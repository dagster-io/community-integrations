from dagster._core.libraries import DagsterLibraryRegistry

from dagster_darkmoon.checks import build_darkmoon_findings_check
from dagster_darkmoon.client import DarkmoonClient, DarkmoonError
from dagster_darkmoon.resources import DarkmoonResource

__version__ = "0.0.1"

DagsterLibraryRegistry.register(
    "dagster-darkmoon", __version__, is_dagster_package=False
)

__all__ = [
    "DarkmoonClient",
    "DarkmoonError",
    "DarkmoonResource",
    "build_darkmoon_findings_check",
]

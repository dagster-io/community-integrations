from dagster._core.libraries import DagsterLibraryRegistry

from dagster_nebius.endpoints import NebiusEndpointResource as NebiusEndpointResource
from dagster_nebius.pipes import PipesNebiusClient as PipesNebiusClient

__version__ = "0.1.0"

DagsterLibraryRegistry.register("dagster-nebius", __version__, is_dagster_package=False)

from dagster._core.libraries import DagsterLibraryRegistry

from dagster_vertexai.resource import VertexAIResource as VertexAIResource

__version__ = "0.0.1"

DagsterLibraryRegistry.register(
    "dagster-vertexai", __version__, is_dagster_package=False
)

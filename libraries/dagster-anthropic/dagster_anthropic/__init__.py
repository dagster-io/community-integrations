from dagster._core.libraries import DagsterLibraryRegistry

from dagster_anthropic.resource import AnthropicResource as AnthropicResource

__version__ = "0.0.5"

DagsterLibraryRegistry.register(
    "dagster-anthropic", __version__, is_dagster_package=False
)

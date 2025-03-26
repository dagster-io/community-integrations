from dagster._core.libraries import DagsterLibraryRegistry

from dagster_anthropic.resource import AnthropicResource as AnthropicResource

__version__ = "0.0.2"

DagsterLibraryRegistry.register("dagster-anthropic", __version__)

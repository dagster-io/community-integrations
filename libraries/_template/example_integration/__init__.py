from dagster._core.libraries import DagsterLibraryRegistry

__version__ = "0.0.1"

DagsterLibraryRegistry.register("example-integration", __version__)

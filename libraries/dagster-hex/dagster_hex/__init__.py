from dagster._core.libraries import DagsterLibraryRegistry

__version__ = "0.1.7"

DagsterLibraryRegistry.register("dagster-hex", __version__)

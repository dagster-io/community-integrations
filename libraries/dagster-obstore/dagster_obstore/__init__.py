from dagster._core.libraries import DagsterLibraryRegistry

__version__ = "0.2.1"

DagsterLibraryRegistry.register("dagster-obstore", __version__)

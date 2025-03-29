from dagster._core.libraries import DagsterLibraryRegistry

__version__ = "0.2.2"

DagsterLibraryRegistry.register("dagster-obstore", __version__)

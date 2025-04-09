from dagster._core.libraries import DagsterLibraryRegistry

__version__ = "0.1.0"

DagsterLibraryRegistry.register("dagster-evidence", __version__)

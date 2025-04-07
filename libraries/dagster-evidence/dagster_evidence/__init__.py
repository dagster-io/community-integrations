from dagster._core.libraries import DagsterLibraryRegistry

__version__ = "0.26.9"

DagsterLibraryRegistry.register("dagster-evidence", __version__)

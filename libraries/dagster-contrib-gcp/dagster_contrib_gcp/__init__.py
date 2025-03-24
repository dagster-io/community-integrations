from dagster._core.libraries import DagsterLibraryRegistry

__version__ = "0.0.4"

DagsterLibraryRegistry.register("dagster-contrib-gcp", __version__)

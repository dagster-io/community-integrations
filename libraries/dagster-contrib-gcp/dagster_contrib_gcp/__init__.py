from dagster._core.libraries import DagsterLibraryRegistry

__version__ = "0.0.5"

DagsterLibraryRegistry.register("dagster-contrib-gcp", __version__)

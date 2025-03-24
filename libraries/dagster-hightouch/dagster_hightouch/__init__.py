from dagster._core.libraries import DagsterLibraryRegistry

__version__ = "0.1.5"

DagsterLibraryRegistry.register("dagster-hightouch", __version__)

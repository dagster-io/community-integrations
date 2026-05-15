from dagster._core.libraries import DagsterLibraryRegistry

__version__ = "0.1.9"

DagsterLibraryRegistry.register("dagster-hex", __version__, is_dagster_package=False)

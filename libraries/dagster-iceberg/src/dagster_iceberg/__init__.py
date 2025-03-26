from dagster._core.libraries import DagsterLibraryRegistry

from dagster_iceberg.version import __version__ as __version__

DagsterLibraryRegistry.register("dagster-iceberg", __version__)

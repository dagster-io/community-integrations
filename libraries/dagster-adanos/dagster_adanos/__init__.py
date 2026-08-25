from dagster._core.libraries import DagsterLibraryRegistry

from dagster_adanos.resource import AdanosResource as AdanosResource

__version__ = "0.0.1"

DagsterLibraryRegistry.register("dagster-adanos", __version__, is_dagster_package=False)

from dagster._core.libraries import DagsterLibraryRegistry

from dagster_chroma.resource import (
    ChromaResource as ChromaResource,
)

from dagster_chroma.config import (
    LocalConfig as LocalConfig,
    HttpConfig as HttpConfig,
)

__version__ = "0.0.4"

DagsterLibraryRegistry.register("dagster-chroma", __version__, is_dagster_package=False)

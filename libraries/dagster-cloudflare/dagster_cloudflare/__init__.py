from dagster._core.libraries import DagsterLibraryRegistry

from dagster_cloudflare.containers import CloudflareContainerRunLauncher

__version__ = "0.0.1"

DagsterLibraryRegistry.register(
    "dagster-cloudflare", __version__, is_dagster_package=False
)

__all__ = ["CloudflareContainerRunLauncher"]

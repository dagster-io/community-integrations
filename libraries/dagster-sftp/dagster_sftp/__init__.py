import importlib.metadata

from dagster._core.libraries import DagsterLibraryRegistry

from dagster_sftp.resource import (
    SFTPFileInfo as SFTPFileInfo,
    SFTPFileInfoConfig as SFTPFileInfoConfig,
    SFTPResource as SFTPResource,
)

try:
    __version__ = importlib.metadata.version(__name__)
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.0.0"

DagsterLibraryRegistry.register("dagster-sftp", __version__, is_dagster_package=False)

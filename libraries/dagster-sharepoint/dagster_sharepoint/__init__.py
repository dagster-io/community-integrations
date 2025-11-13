import importlib.metadata

from dagster._core.libraries import DagsterLibraryRegistry

from dagster_sharepoint.resource import (
    DriveInfo as DriveInfo,
    FileInfo as FileInfo,
    FileInfoConfig as FileInfoConfig,
    FolderInfo as FolderInfo,
    SharePointResource as SharePointResource,
    UploadResult as UploadResult,
)

try:
    __version__ = importlib.metadata.version(__name__)
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.0.0"

DagsterLibraryRegistry.register(
    "dagster-sharepoint", __version__, is_dagster_package=False
)

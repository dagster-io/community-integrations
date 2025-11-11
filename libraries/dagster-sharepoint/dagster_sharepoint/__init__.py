from dagster._core.libraries import DagsterLibraryRegistry

from dagster_sharepoint.resource import (
    SharePointResource as SharePointResource,
    FileInfoConfig as FileInfoConfig,
    FileInfo as FileInfo,
    FolderInfo as FolderInfo,
    DriveInfo as DriveInfo,
    UploadResult as UploadResult,
)

__version__ = "0.0.1"

DagsterLibraryRegistry.register(
    "dagster-sharepoint", __version__, is_dagster_package=False
)

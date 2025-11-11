from dagster._core.libraries import DagsterLibraryRegistry

from dagster_sftp.resource import (
    SFTPResource as SFTPResource,
    SFTPFileInfo as SFTPFileInfo,
    SFTPFileInfoConfig as SFTPFileInfoConfig,
)

__version__ = "0.0.2"

DagsterLibraryRegistry.register("dagster-sftp", __version__, is_dagster_package=False)

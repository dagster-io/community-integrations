"""AsyncSSH-based SFTP Resource for Dagster.

This module provides a synchronous SFTP resource that internally uses asyncSSH,
offering cleaner implementation with built-in features for:
- Parallel transfers with max_concurrent_requests
- Progress callbacks for monitoring
- Pattern matching with glob
- Batch operations with parallel downloads
- Efficient directory traversal with scandir
"""

import stat
from datetime import datetime
from pathlib import Path

import anyio
import asyncssh
import dagster as dg
from pydantic import Field
from dagster_shared.record import record
from asyncssh.sftp import (
    _SFTPPaths,
    _SFTPPath,
    SFTPErrorHandler,
    SFTPProgressHandler,
    SFTPNoSuchFile,
    SFTPNoSuchPath,
)

logger = dg.get_dagster_logger(__name__)


class SFTPFileInfoConfig(dg.Config):
    """Configuration for SFTPFileInfo."""

    filename: str
    path: str
    size: int
    is_dir: bool
    is_file: bool
    is_link: bool
    mode: int
    modified_time: str
    accessed_time: str | None = None
    owner: int | None = None
    group: int | None = None


@record
class SFTPFileInfo:
    """Information about a file or directory on the SFTP server."""

    filename: str
    path: str
    size: int
    is_dir: bool
    is_file: bool
    is_link: bool
    mode: int
    modified_time: datetime
    accessed_time: datetime | None = None
    owner: int | None = None
    group: int | None = None

    def should_include_file(
        self,
        files_only: bool,
        dirs_only: bool,
        modified_after: datetime | None,
        modified_before: datetime | None,
        accessed_after: datetime | None,
        accessed_before: datetime | None,
    ) -> bool:
        """Check if a file should be included based on filters."""
        # Type filters
        if files_only and not self.is_file:
            return False
        if dirs_only and not self.is_dir:
            return False

        # Time filters
        if modified_after and self.modified_time < modified_after:
            return False
        if modified_before and self.modified_time > modified_before:
            return False
        if (
            accessed_after
            and self.accessed_time
            and self.accessed_time < accessed_after
        ):
            return False
        if (
            accessed_before
            and self.accessed_time
            and self.accessed_time > accessed_before
        ):
            return False

        return True

    @classmethod
    def from_sftp_attrs(
        cls, path: str, filename: str, attrs: asyncssh.SFTPAttrs
    ) -> "SFTPFileInfo":
        """Create SFTPFileInfo from asyncssh SFTPAttrs."""
        permissions = attrs.permissions or 0
        is_dir = stat.S_ISDIR(permissions)
        is_file = stat.S_ISREG(permissions)
        is_link = stat.S_ISLNK(permissions)

        return cls(
            filename=filename,
            path=path,
            size=attrs.size or 0,
            is_dir=is_dir,
            is_file=is_file,
            is_link=is_link,
            mode=permissions,
            modified_time=datetime.fromtimestamp(attrs.mtime)
            if attrs.mtime
            else datetime.now(),
            accessed_time=datetime.fromtimestamp(attrs.atime) if attrs.atime else None,
            owner=attrs.uid,
            group=attrs.gid,
        )

    def to_config_dict(self) -> dict:
        """Convert to SFTPFileInfo to a dict for config serialization."""
        return SFTPFileInfoConfig(
            filename=self.filename,
            path=self.path,
            size=self.size,
            is_dir=self.is_dir,
            is_file=self.is_file,
            is_link=self.is_link,
            mode=self.mode,
            modified_time=self.modified_time.isoformat(),
            accessed_time=self.accessed_time.isoformat()
            if self.accessed_time
            else None,
            owner=self.owner,
        ).model_dump()


class SFTPResource(dg.ConfigurableResource):
    """A Dagster resource for SFTP operations using asyncSSH internally.

    This resource provides comprehensive SFTP functionality with a synchronous API
    while leveraging asyncSSH's async features internally for optimal performance.
    It supports parallel chunk transfers, concurrent operations, pattern matching,
    and batch operations.

    The resource uses asyncSSH under the hood, which provides:

    - Automatic parallelization of large file transfers
    - Configurable concurrent request limits
    - Progress monitoring callbacks
    - Robust error handling
    - Support for both password and key-based authentication

    Example:
        Basic usage with password authentication:

        .. code-block:: python

            from dagster import Definitions, asset
            from dagster_sftp import SFTPResource

            @asset
            def download_files(sftp: SFTPResource):
                # List CSV files modified in the last week
                files = sftp.list_files(
                    "/data/exports",
                    pattern="*.csv",
                    modified_after=datetime.now() - timedelta(days=7)
                )

                # Download each file
                for file_info in files:
                    sftp.get_file(file_info.path, f"/local/data/{file_info.name}")

                return {"files_downloaded": len(files)}

            defs = Definitions(
                assets=[download_files],
                resources={
                    "sftp": SFTPResource(
                        host="sftp.example.com",
                        username="myuser",
                        password="mypassword"
                    )
                }
            )

        Using key-based authentication with file path:

        .. code-block:: python

            sftp_resource = SFTPResource(
                host="sftp.example.com",
                username="myuser",
                private_key_path="/path/to/private/key",
                passphrase="key_passphrase",  # Optional
                known_hosts="/path/to/known_hosts"
            )

        Using key-based authentication with key content:

        .. code-block:: python

            with open("/path/to/private/key", "r") as f:
                key_content = f.read()

            sftp_resource = SFTPResource(
                host="sftp.example.com",
                username="myuser",
                private_key_content=key_content,
                passphrase="key_passphrase",  # Optional
                known_hosts="/path/to/known_hosts"
            )

    :param host: The hostname or IP address of the SFTP server
    :type host: str
    :param port: The port number for SFTP connection
    :type port: int
    :param username: The username for authentication
    :type username: str
    :param password: The password for authentication (if not using key-based auth)
    :type password: str or None
    :param private_key_path: Path to private key file for authentication
    :type private_key_path: str or None
    :param private_key_content: Private key content as a string for authentication
    :type private_key_content: str or None
    :param passphrase: Passphrase for the private key (if encrypted)
    :type passphrase: str or None
    :param known_hosts: Path to known hosts file or None to disable host key checking
    :type known_hosts: str or None
    :param keepalive_interval: Seconds between keepalive packets to prevent timeout
    :type keepalive_interval: int
    :param connect_timeout: Connection timeout in seconds
    :type connect_timeout: int
    :param default_max_requests: Maximum concurrent SFTP requests for parallel operations (-1 for server default)
    :type default_max_requests: int
    :param default_block_size: Default block size for parallel transfers (-1 for server default)
    :type default_block_size: int
    """

    host: str = Field(description="The hostname or IP address of the SFTP server")
    port: int = Field(default=22, description="The port number")
    username: str = Field(description="The username for authentication")
    password: str | None = Field(
        default=None, description="The password for authentication"
    )
    private_key_path: str | None = Field(
        default=None, description="Path to private key file for authentication"
    )
    private_key_content: str | None = Field(
        default=None, description="Private key content as a string for authentication"
    )
    passphrase: str | None = Field(
        default=None, description="Passphrase for the private key"
    )
    known_hosts: str | None = Field(
        default=None,
        description="Path to known hosts file or None to disable host key checking",
    )
    keepalive_interval: int = Field(
        default=15,
        description="Send a keepalive packet to remote host every keepalive_interval seconds",
    )
    connect_timeout: int = Field(
        default=60, description="Connection timeout in seconds"
    )
    default_max_requests: int = Field(
        default=-1,
        description="Maximum concurrent SFTP requests for parallel operations",
    )
    default_block_size: int = Field(
        default=-1, description="Default block size for parallel operations"
    )

    def error_handler(self, exc: Exception) -> None:
        """Default error handler for SFTP operations.

        This method is called when an error occurs during SFTP operations like
        glob, get, put, copy, or rmtree. It logs the exception with full stack
        trace and re-raises it. You can override this behavior by providing a
        custom error_handler parameter to individual methods.

        :param exc: The exception that occurred during the SFTP operation
        :type exc: Exception
        :raises: Re-raises the exception after logging it

        Example:
            Custom error handler for glob operations:

            .. code-block:: python

                def custom_handler(exc):
                    if isinstance(exc, asyncssh.SFTPNoSuchFile):
                        logger.warning(f"File not found: {exc}")
                        return  # Continue processing
                    raise exc  # Re-raise other errors

                files = sftp.list_files(
                    "/path",
                    pattern="*.txt",
                    error_handler=custom_handler
                )
        """
        if (
            isinstance(exc, (SFTPNoSuchPath, SFTPNoSuchFile))
            and exc.reason == "No matches found"
        ):
            if hasattr(exc, "srcpath"):
                logger.warning(f"No files found matching pattern: {exc.srcpath}")  # pyright: ignore[reportAttributeAccessIssue]
            else:
                logger.warning("No files found matching pattern")
            return

        logger.exception(exc, stack_info=True)
        raise exc

    def progress_handler(
        self, src: bytes, dest: bytes, current: int, total: int
    ) -> None:
        """Default progress handler for file transfer operations.

        This method is called periodically during file transfers (get, put, copy)
        to report progress. It logs the transfer progress at debug level. You can
        override this behavior by providing a custom progress_handler parameter
        to individual transfer methods.

        :param src: Source file path (as bytes)
        :type src: bytes
        :param dest: Destination file path (as bytes)
        :type dest: bytes
        :param current: Number of bytes transferred so far
        :type current: int
        :param total: Total number of bytes to transfer (0 if unknown)
        :type total: int
        :return: None
        :rtype: None

        Example:
            Custom progress handler with a progress bar:

            .. code-block:: python

                from tqdm import tqdm

                class ProgressTracker:
                    def __init__(self):
                        self.pbar = None

                    def __call__(self, src, dest, current, total):
                        if self.pbar is None and total > 0:
                            self.pbar = tqdm(total=total, unit='B', unit_scale=True)
                        if self.pbar:
                            self.pbar.update(current - self.pbar.n)
                        if current == total and self.pbar:
                            self.pbar.close()
                            self.pbar = None

                tracker = ProgressTracker()
                sftp.get_file(
                    "/remote/largefile.zip",
                    "/local/largefile.zip",
                    progress_handler=tracker
                )
        """
        if total > 0:
            percent = (current / total) * 100
            logger.debug(f" {src} -> {dest} | {percent:.1f}% ({current}/{total} bytes)")

    def list_files(
        self,
        base_path: _SFTPPath = ".",
        pattern: str | None = None,
        files_only: bool = False,
        dirs_only: bool = False,
        follow_symlinks: bool = True,
        modified_after: datetime | None = None,
        modified_before: datetime | None = None,
        accessed_after: datetime | None = None,
        accessed_before: datetime | None = None,
        error_handler: SFTPErrorHandler | None = None,
    ) -> list[SFTPFileInfo]:
        """List files and directories with advanced filtering capabilities.

        Uses asyncSSH's glob functionality to efficiently search for files matching
        specified criteria. Supports recursive searching, pattern matching, and
        filtering by file attributes like modification time.

        :param base_path: Base directory path to start searching from
        :type base_path: str or Path
        :param pattern: Unix-style glob pattern for matching files. Supports wildcards:
                        * - matches any characters except /
                        ? - matches single character
                        ** - matches any characters including / (recursive)
                        [...] - matches character ranges
        :type pattern: str or None
        :param files_only: If True, only return regular files
        :type files_only: bool
        :param dirs_only: If True, only return directories
        :type dirs_only: bool
        :param follow_symlinks: Whether to follow symbolic links when checking attributes
        :type follow_symlinks: bool
        :param modified_after: Only include files modified after this datetime
        :type modified_after: datetime or None
        :param modified_before: Only include files modified before this datetime
        :type modified_before: datetime or None
        :param accessed_after: Only include files accessed after this datetime
        :type accessed_after: datetime or None
        :param accessed_before: Only include files accessed before this datetime
        :type accessed_before: datetime or None
        :param error_handler: Custom error handler for handling SFTP errors
        :type error_handler: SFTPErrorHandler or None
        :return: List of SFTPFileInfo objects matching the specified criteria
        :rtype: list[SFTPFileInfo]
        :raises ValueError: If both files_only and dirs_only are True
        :raises asyncssh.SFTPNoSuchFile: If base_path doesn't exist

        Example:
            List all CSV files modified in the last week:

            .. code-block:: python

                from datetime import datetime, timedelta

                recent_csvs = sftp.list_files(
                    base_path="/data/exports",
                    pattern="*.csv",
                    files_only=True,
                    modified_after=datetime.now() - timedelta(days=7)
                )

                for file in recent_csvs:
                    print(f"{file.name}: {file.size} bytes, modified {file.mtime}")

            Recursively find all Python files:

            .. code-block:: python

                python_files = sftp.list_files(
                    base_path="/project",
                    pattern="**/*.py",
                    files_only=True
                )

            List only directories with custom error handling:

            .. code-block:: python

                def handle_error(exc):
                    logger.warning(f"Error accessing path: {exc}")
                    # Return None to continue, or raise to stop

                dirs = sftp.list_files(
                    base_path="/home",
                    dirs_only=True,
                    error_handler=handle_error
                )
        """

        if files_only and dirs_only:
            raise ValueError("Cannot set both files_only and dirs_only to True")

        base_path_str = str(base_path).rstrip("/") or "."

        if pattern:
            full_pattern = (
                f"{base_path_str}/{pattern}" if base_path_str != "." else pattern
            )
        else:
            # If no pattern specified, list everything in the directory
            full_pattern = f"{base_path_str}/*" if base_path_str != "." else "*"

        async def _inner():
            async with (
                asyncssh.connect(**self._connect_args) as conn,
                conn.start_sftp_client() as sftp,
            ):
                results = []
                matched_paths = await sftp.glob(
                    full_pattern, error_handler=error_handler or self.error_handler
                )

                for path in matched_paths:
                    try:
                        attrs = await sftp.stat(path, follow_symlinks=follow_symlinks)

                        # Convert path to string if it's bytes
                        path_str = (
                            path.decode("utf-8")
                            if isinstance(path, bytes)
                            else str(path)
                        )
                        filename = Path(path_str).name
                        file_info = SFTPFileInfo.from_sftp_attrs(
                            path_str, filename, attrs
                        )

                        if file_info.should_include_file(
                            files_only=files_only,
                            dirs_only=dirs_only,
                            modified_after=modified_after,
                            modified_before=modified_before,
                            accessed_after=accessed_after,
                            accessed_before=accessed_before,
                        ):
                            results.append(file_info)
                    except (asyncssh.SFTPError, OSError) as exc:
                        if callable(error_handler):
                            error_handler(exc)

                return results

        return anyio.run(_inner)

    def get_file(
        self,
        remote_path: _SFTPPaths,
        local_path: _SFTPPath,
        *,
        preserve: bool = False,
        follow_symlinks: bool = True,
        max_requests: int | None = None,
        block_size: int | None = None,
        progress_handler: SFTPProgressHandler | None = None,
        error_handler: SFTPErrorHandler | None = None,
    ) -> Path:
        """Download files from the SFTP server with automatic parallelization.

        Downloads one or more files from the remote server to local filesystem.
        Large files are automatically downloaded in parallel chunks for optimal
        performance. The method supports preserving file attributes, following
        symlinks, and progress monitoring.

        :param remote_path: Path to remote file(s). Can be:
                           - Single path (str or Path)
                           - List of specific paths for batch download
                           Note: Does NOT support glob patterns (use mget for that)
        :type remote_path: str, Path, or list
        :param local_path: Local destination path. Can be:
                          - File path (if downloading single file)
                          - Directory path (if downloading multiple files)
                          - Target filename (for single file rename)
        :type local_path: str or Path
        :param preserve: If True, preserve file permissions and timestamps
        :type preserve: bool
        :param follow_symlinks: If True, follow symbolic links on remote server
        :type follow_symlinks: bool
        :param max_requests: Maximum concurrent requests for parallel transfer.
                            None uses the default_max_requests setting
        :type max_requests: int or None
        :param block_size: Block size for parallel transfers in bytes.
                          None uses the default_block_size setting
        :type block_size: int or None
        :param progress_handler: Callback for progress updates. Called with
                                (src, dest, current_bytes, total_bytes)
        :type progress_handler: SFTPProgressHandler or None
        :param error_handler: Custom error handler for transfer errors
        :type error_handler: SFTPErrorHandler or None
        :return: Path to the downloaded file(s)
        :rtype: Path
        :raises asyncssh.SFTPNoSuchFile: If remote file doesn't exist
        :raises asyncssh.SFTPPermissionDenied: If lacking read permissions
        :raises asyncssh.SFTPError: For other SFTP-related errors
        :raises OSError: For local filesystem errors

        Example:
            Download a single file:

            .. code-block:: python

                # Simple download
                local_file = sftp.get_file(
                    "/remote/data.csv",
                    "/local/data.csv"
                )

                # Download with preserved attributes
                sftp.get_file(
                    "/remote/script.sh",
                    "/local/script.sh",
                    preserve=True  # Keeps permissions and timestamps
                )

            Download multiple files:

            .. code-block:: python

                # Download specific files to a directory
                files_to_download = [
                    "/remote/exports/data1.csv",
                    "/remote/exports/data2.csv"
                ]
                sftp.get_file(files_to_download, "/local/downloads/")

                # Download with progress tracking
                def show_progress(src, dest, current, total):
                    percent = (current / total * 100) if total > 0 else 0
                    print(f"Downloading {src}: {percent:.1f}%")

                sftp.get_file(
                    ["/remote/file1.txt", "/remote/file2.txt"],
                    "/local/files/",
                    progress_handler=show_progress
                )

            Optimize transfer for large files:

            .. code-block:: python

                # Download large file with custom parallelization
                sftp.get_file(
                    "/remote/largefile.zip",
                    "/local/largefile.zip",
                    max_requests=256,  # Increase parallel chunks
                    block_size=65536   # 64KB blocks
                )
        """

        async def _inner():
            async with (
                asyncssh.connect(**self._connect_args) as conn,
                conn.start_sftp_client() as sftp,
            ):
                await sftp.get(
                    remotepaths=remote_path,
                    localpath=local_path,
                    preserve=preserve,
                    follow_symlinks=follow_symlinks,
                    max_requests=max_requests or self.default_max_requests,
                    block_size=block_size or self.default_block_size,
                    progress_handler=progress_handler or self.progress_handler,
                    error_handler=error_handler or self.error_handler,
                )

        anyio.run(_inner)
        # Return the local path where files were downloaded
        if local_path:
            # Convert to string if bytes
            if isinstance(local_path, bytes):
                return Path(local_path.decode("utf-8"))
            else:
                return Path(local_path)
        else:
            # If no local path specified, files are downloaded to current directory
            return Path.cwd()

    def put_file(
        self,
        local_path: _SFTPPaths,
        remote_path: _SFTPPath,
        *,
        preserve: bool = False,
        follow_symlinks: bool = True,
        max_requests: int | None = None,
        block_size: int | None = None,
        progress_handler: SFTPProgressHandler | None = None,
        error_handler: SFTPErrorHandler | None = None,
    ) -> str:
        """Upload files to the SFTP server with automatic parallelization.

        Uploads one or more files from local filesystem to the remote server.
        Large files are automatically uploaded in parallel chunks for optimal
        performance. The method supports preserving file attributes, following
        symlinks, and progress monitoring.

        :param local_path: Path to local file(s). Can be:
                          - Single file path (str or Path)
                          - List of specific file paths for batch upload
                          Note: Does NOT support glob patterns
        :type local_path: str, Path, or list
        :param remote_path: Remote destination path. Can be:
                           - File path (if uploading single file)
                           - Directory path (if uploading multiple files)
                           - Target filename (for single file rename)
        :type remote_path: str or Path
        :param preserve: If True, preserve file permissions and timestamps
        :type preserve: bool
        :param follow_symlinks: If True, follow symbolic links on local system
        :type follow_symlinks: bool
        :param max_requests: Maximum concurrent requests for parallel transfer.
                            None uses the default_max_requests setting
        :type max_requests: int or None
        :param block_size: Block size for parallel transfers in bytes.
                          None uses the default_block_size setting
        :type block_size: int or None
        :param progress_handler: Callback for progress updates. Called with
                                (src, dest, current_bytes, total_bytes)
        :type progress_handler: SFTPProgressHandler or None
        :param error_handler: Custom error handler for transfer errors
        :type error_handler: SFTPErrorHandler or None
        :return: The remote path where file(s) were uploaded
        :rtype: str
        :raises asyncssh.SFTPNoSuchFile: If local file doesn't exist
        :raises asyncssh.SFTPPermissionDenied: If lacking write permissions on server
        :raises asyncssh.SFTPError: For other SFTP-related errors
        :raises OSError: For local filesystem errors

        Example:
            Upload a single file:

            .. code-block:: python

                # Simple upload
                remote_file = sftp.put_file(
                    "/local/report.pdf",
                    "/remote/reports/report.pdf"
                )

                # Upload with preserved attributes
                sftp.put_file(
                    "/local/executable.sh",
                    "/remote/bin/executable.sh",
                    preserve=True  # Keeps permissions and timestamps
                )

            Upload multiple files:

            .. code-block:: python

                # Upload specific files to remote directory
                sftp.put_file(
                    ["/local/file1.log", "/local/file2.log"],
                    "/remote/logs/"
                )

                # Upload multiple files with progress tracking
                def show_progress(src, dest, current, total):
                    percent = (current / total * 100) if total > 0 else 0
                    print(f"Uploading {src}: {percent:.1f}%")

                sftp.put_file(
                    ["/local/file1.txt", "/local/file2.txt"],
                    "/remote/uploads/",
                    progress_handler=show_progress
                )

            Optimize upload for large files:

            .. code-block:: python

                # Upload large file with custom parallelization
                sftp.put_file(
                    "/local/backup.tar.gz",
                    "/remote/backups/backup.tar.gz",
                    max_requests=256,  # Increase parallel chunks
                    block_size=131072  # 128KB blocks
                )

                # Upload with error handling
                def handle_upload_error(exc):
                    if isinstance(exc, asyncssh.SFTPPermissionDenied):
                        logger.error(f"Permission denied: {exc}")
                        raise
                    logger.warning(f"Upload warning: {exc}")

                sftp.put_file(
                    "/local/data.csv",
                    "/remote/data.csv",
                    error_handler=handle_upload_error
                )
        """

        async def _inner():
            async with (
                asyncssh.connect(**self._connect_args) as conn,
                conn.start_sftp_client() as sftp,
            ):
                await sftp.put(
                    localpaths=local_path,
                    remotepath=remote_path,
                    preserve=preserve,
                    follow_symlinks=follow_symlinks,
                    max_requests=max_requests or self.default_max_requests,
                    block_size=block_size or self.default_block_size,
                    progress_handler=progress_handler or self.progress_handler,
                    error_handler=error_handler or self.error_handler,
                )

        anyio.run(_inner)
        # Return the remote path where files were uploaded
        if remote_path:
            return str(remote_path)
        else:
            # If no remote path specified, files are uploaded to current remote directory
            return "."

    def delete_file(self, remote_path: _SFTPPath) -> None:
        """Delete a single file from the SFTP server.

        Removes a file from the remote server. This method only works with
        regular files; use rmtree() to remove directories.

        :param remote_path: Path to the file to delete
        :type remote_path: str or Path
        :return: None
        :rtype: None
        :raises asyncssh.SFTPNoSuchFile: If the file doesn't exist
        :raises asyncssh.SFTPPermissionDenied: If lacking delete permissions
        :raises asyncssh.SFTPError: If path is a directory or other SFTP errors

        Example:
            Delete a single file:

            .. code-block:: python

                # Delete a file
                sftp.delete_file("/remote/temp/old_data.csv")

                # Delete with existence check
                if sftp.file_exists("/remote/temp.txt"):
                    sftp.delete_file("/remote/temp.txt")

                # Delete with error handling
                try:
                    sftp.delete_file("/remote/file.txt")
                except asyncssh.SFTPNoSuchFile:
                    print("File already deleted")
        """

        async def _inner():
            async with (
                asyncssh.connect(**self._connect_args) as conn,
                conn.start_sftp_client() as sftp,
            ):
                return await sftp.remove(remote_path)

        return anyio.run(_inner)

    def delete_files(
        self,
        remote_paths: list[_SFTPPath],
    ) -> None:
        """Delete multiple files from the SFTP server concurrently.

        Efficiently deletes multiple files using parallel operations. This method
        is significantly faster than calling delete_file() repeatedly for large
        numbers of files, as it performs deletions concurrently.

        :param remote_paths: List of file paths to delete
        :type remote_paths: list[str or Path]
        :return: None
        :rtype: None
        :raises asyncssh.SFTPNoSuchFile: If any file doesn't exist
        :raises asyncssh.SFTPPermissionDenied: If lacking delete permissions
        :raises asyncssh.SFTPError: For other SFTP-related errors

        Example:
            Delete multiple files:

            .. code-block:: python

                # Delete a list of files
                files_to_delete = [
                    "/remote/temp/file1.txt",
                    "/remote/temp/file2.txt",
                    "/remote/temp/file3.txt"
                ]
                sftp.delete_files(files_to_delete)

                # Delete files matching a pattern
                files = sftp.list_files(
                    "/remote/logs",
                    pattern="*.log",
                    modified_before=datetime.now() - timedelta(days=30)
                )
                sftp.delete_files([f.path for f in files])

                # Delete with error handling
                try:
                    sftp.delete_files([
                        "/remote/file1.txt",
                        "/remote/file2.txt"
                    ])
                except asyncssh.SFTPError as e:
                    logger.error(f"Failed to delete some files: {e}")
        """

        async def _inner():
            async with (
                asyncssh.connect(**self._connect_args) as conn,
                conn.start_sftp_client() as sftp,
                anyio.create_task_group() as tg,
            ):
                for filepath in remote_paths:
                    tg.start_soon(sftp.remove, filepath)

        return anyio.run(_inner)

    def file_exists(self, remote_path: _SFTPPath) -> bool:
        """Check if a file or directory exists on the SFTP server.

        Tests whether a given path exists on the remote server. Works for both
        files and directories.

        :param remote_path: Path to check for existence
        :type remote_path: str or Path
        :return: True if the path exists, False otherwise
        :rtype: bool

        Example:
            Check file existence:

            .. code-block:: python

                # Check if file exists before downloading
                if sftp.file_exists("/remote/data.csv"):
                    sftp.get_file("/remote/data.csv", "/local/data.csv")
                else:
                    print("File not found")

                # Check directory existence
                if not sftp.file_exists("/remote/backup"):
                    sftp.mkdir("/remote/backup")

                # Conditional processing
                config_exists = sftp.file_exists("/remote/config.json")
                if config_exists:
                    # Process with config
                    pass
                else:
                    # Use defaults
                    pass
        """

        async def _inner():
            async with (
                asyncssh.connect(**self._connect_args) as conn,
                conn.start_sftp_client() as sftp,
            ):
                return await sftp.exists(remote_path)

        return anyio.run(_inner)

    def mkdir(
        self, remote_path: _SFTPPath, mode: int = 0o755, exist_ok: bool = True
    ) -> None:
        """Create a directory on the SFTP server, including parent directories.

        Creates the specified directory and any necessary parent directories.
        Similar to `mkdir -p` in Unix. By default, does not raise an error if
        the directory already exists.

        :param remote_path: Path of the directory to create
        :type remote_path: str or Path
        :param mode: Unix permission mode for the new directory
        :type mode: int
        :param exist_ok: If True, don't raise error if directory exists
        :type exist_ok: bool
        :return: None
        :rtype: None
        :raises asyncssh.SFTPPermissionDenied: If lacking create permissions
        :raises asyncssh.SFTPFileAlreadyExists: If exist_ok=False and directory exists
        :raises asyncssh.SFTPError: For other SFTP-related errors

        Example:
            Create directories:

            .. code-block:: python

                # Create a single directory
                sftp.mkdir("/remote/new_folder")

                # Create nested directories (creates all parents)
                sftp.mkdir("/remote/path/to/deep/folder")

                # Create with specific permissions
                sftp.mkdir("/remote/scripts", mode=0o775)

                # Fail if directory exists
                try:
                    sftp.mkdir("/remote/existing", exist_ok=False)
                except asyncssh.SFTPFileAlreadyExists:
                    print("Directory already exists")

                # Create directory structure for organized uploads
                base_dir = "/remote/data/2024"
                for month in range(1, 13):
                    sftp.mkdir(f"{base_dir}/{month:02d}")
        """

        async def _inner():
            async with (
                asyncssh.connect(**self._connect_args) as conn,
                conn.start_sftp_client() as sftp,
            ):
                await sftp.makedirs(
                    path=remote_path,
                    attrs=asyncssh.SFTPAttrs(mode=mode),
                    exist_ok=exist_ok,
                )

        return anyio.run(_inner)

    def rmtree(self, remote_path: _SFTPPath) -> None:
        """Recursively remove a directory tree from the SFTP server.

        Removes a directory and all its contents, including subdirectories and
        files. Similar to `rm -rf` in Unix. Use with caution as this operation
        cannot be undone.

        :param remote_path: Path of the directory tree to remove
        :type remote_path: str or Path
        :return: None
        :rtype: None
        :raises asyncssh.SFTPNoSuchFile: If the directory doesn't exist
        :raises asyncssh.SFTPPermissionDenied: If lacking delete permissions
        :raises asyncssh.SFTPError: For other SFTP-related errors

        Example:
            Remove directory trees:

            .. code-block:: python

                # Remove a directory and all its contents
                sftp.rmtree("/remote/old_project")

                # Remove with existence check
                if sftp.is_dir("/remote/temp"):
                    sftp.rmtree("/remote/temp")

                # Clean up old backups
                backup_dirs = sftp.list_files(
                    "/remote/backups",
                    dirs_only=True,
                    modified_before=datetime.now() - timedelta(days=90)
                )
                for dir_info in backup_dirs:
                    sftp.rmtree(dir_info.path)

                # Remove with error handling
                try:
                    sftp.rmtree("/remote/protected")
                except asyncssh.SFTPPermissionDenied as e:
                    logger.error(f"Cannot remove protected directory: {e}")
        """

        async def _inner():
            async with (
                asyncssh.connect(**self._connect_args) as conn,
                conn.start_sftp_client() as sftp,
            ):
                return await sftp.rmtree(remote_path)

        return anyio.run(_inner)

    def is_dir(self, remote_path: _SFTPPath) -> bool:
        """Check if a path points to a directory.

        Tests whether the specified path exists and is a directory.
        Returns False if the path doesn't exist or is not a directory.

        :param remote_path: Path to check
        :type remote_path: str or Path
        :return: True if path is a directory, False otherwise
        :rtype: bool

        Example:
            Check directory type:

            .. code-block:: python

                # Check before operations
                if sftp.is_dir("/remote/uploads"):
                    # List contents
                    files = sftp.list_files("/remote/uploads")
                else:
                    # Create directory
                    sftp.mkdir("/remote/uploads")

                # Differentiate files from directories
                for item in sftp.list_files("/remote"):
                    if sftp.is_dir(item.path):
                        print(f"Directory: {item.name}/")
                    else:
                        print(f"File: {item.name}")
        """

        async def _inner():
            async with (
                asyncssh.connect(**self._connect_args) as conn,
                conn.start_sftp_client() as sftp,
            ):
                return await sftp.isdir(remote_path)

        return anyio.run(_inner)

    def is_file(self, remote_path: _SFTPPath) -> bool:
        """Check if a path points to a regular file.

        Tests whether the specified path exists and is a regular file.
        Returns False if the path doesn't exist or is not a regular file
        (e.g., if it's a directory or symbolic link).

        :param remote_path: Path to check
        :type remote_path: str or Path
        :return: True if path is a regular file, False otherwise
        :rtype: bool

        Example:
            Check file type:

            .. code-block:: python

                # Check before download
                if sftp.is_file("/remote/data.csv"):
                    sftp.get_file("/remote/data.csv", "/local/data.csv")
                else:
                    print("Not a regular file")

                # Process only files, not directories
                for item in sftp.list_files("/remote/mixed"):
                    if sftp.is_file(item.path):
                        # Process file
                        print(f"Processing file: {item.name}")
                        sftp.get_file(item.path, f"/local/{item.name}")
        """

        async def _inner():
            async with (
                asyncssh.connect(**self._connect_args) as conn,
                conn.start_sftp_client() as sftp,
            ):
                return await sftp.isfile(remote_path)

        return anyio.run(_inner)

    def rename(self, old_path: _SFTPPath, new_path: _SFTPPath) -> None:
        """Rename or move a file or directory on the SFTP server.

        Renames a file or directory to a new name, or moves it to a different
        location on the same server. The operation is atomic on most SFTP servers.

        Note: Behavior when new_path exists depends on SFTP server version:
        - SFTP v3: Raises an error if new_path exists
        - SFTP v6+: May support flags for overwriting existing files

        :param old_path: Current path of the file or directory
        :type old_path: str or Path
        :param new_path: New path for the file or directory
        :type new_path: str or Path
        :return: None
        :rtype: None
        :raises asyncssh.SFTPNoSuchFile: If old_path doesn't exist
        :raises asyncssh.SFTPFileAlreadyExists: If new_path exists (SFTP v3)
        :raises asyncssh.SFTPPermissionDenied: If lacking rename permissions
        :raises asyncssh.SFTPError: For other SFTP-related errors

        Example:
            Rename and move operations:

            .. code-block:: python

                # Simple rename in same directory
                sftp.rename(
                    "/remote/old_name.txt",
                    "/remote/new_name.txt"
                )

                # Move file to different directory
                sftp.rename(
                    "/remote/temp/file.txt",
                    "/remote/archive/file.txt"
                )

                # Rename directory
                sftp.rename(
                    "/remote/old_folder",
                    "/remote/new_folder"
                )

                # Move and rename simultaneously
                sftp.rename(
                    "/remote/uploads/temp_123.csv",
                    "/remote/processed/data_2024.csv"
                )

                # Rename with conflict handling
                new_name = "/remote/file.txt"
                if sftp.file_exists(new_name):
                    # Add timestamp to avoid conflict
                    from datetime import datetime
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    new_name = f"/remote/file_{timestamp}.txt"
                sftp.rename("/remote/temp.txt", new_name)

                # Batch rename with pattern
                files = sftp.list_files("/remote", pattern="temp_*")
                for file in files:
                    new_name = file.path.replace("temp_", "processed_")
                    sftp.rename(file.path, new_name)
        """

        async def _inner():
            async with (
                asyncssh.connect(**self._connect_args) as conn,
                conn.start_sftp_client() as sftp,
            ):
                return await sftp.rename(old_path, new_path)

        return anyio.run(_inner)

    @property
    def _connect_args(self) -> dict:
        """Build connection arguments for asyncSSH.

        Constructs a dictionary of connection parameters based on the resource's
        configuration. Handles both password and key-based authentication methods.

        :return: Dictionary of connection arguments for asyncSSH.connect()
        :rtype: dict
        """
        # Prepare connection arguments
        connect_args = {
            "host": self.host,
            "port": self.port,
            "username": self.username,
            "known_hosts": self.known_hosts,
            "keepalive_interval": self.keepalive_interval,
            "connect_timeout": self.connect_timeout,
        }

        # Add authentication method
        if self.password:
            connect_args["password"] = self.password
        elif self.private_key_path:
            key = asyncssh.import_private_key(
                Path(self.private_key_path).read_bytes(), passphrase=self.passphrase
            )
            connect_args["client_keys"] = [key]
        elif self.private_key_content:
            key = asyncssh.import_private_key(
                self.private_key_content.encode(), passphrase=self.passphrase
            )
            connect_args["client_keys"] = [key]

        return connect_args

    def test_connection(self) -> dict:
        """Test the SFTP connection and authentication.

        This method attempts to establish a connection to the SFTP server
        and retrieve information about the authenticated session. It can be
        used to verify that credentials are correct and the server is accessible.

        :return: Dictionary containing connection information including:
                 - authenticated_user: The username that was authenticated
                 - server_version: SSH server version string
                 - client_version: SSH client version string
                 - host: Connected host
                 - port: Connected port
        :rtype: dict
        :raises asyncssh.PermissionDenied: If authentication fails
        :raises asyncssh.Error: For SSH-related errors
        :raises Exception: For other connection errors

        Example:
            Test connection before performing operations:

            .. code-block:: python

                sftp = SFTPResource(
                    host="sftp.example.com",
                    username="testuser",
                    private_key_path="/path/to/key"
                )

                # Test the connection
                try:
                    info = sftp.test_connection()
                    print(f"Connected as: {info['authenticated_user']}")
                    print(f"Server: {info['server_version']}")
                except asyncssh.PermissionDenied:
                    print("Authentication failed")
                except Exception as e:
                    print(f"Connection failed: {e}")
        """

        async def _inner() -> dict:
            async with (
                asyncssh.connect(**self._connect_args) as conn,
                conn.start_sftp_client(),
            ):
                # Connection successful, gather information
                return {
                    "authenticated_user": conn.get_extra_info("username"),
                    "server_version": conn.get_extra_info("server_version"),
                    "client_version": conn.get_extra_info("client_version"),
                    "host": conn.get_extra_info("host"),
                    "port": conn.get_extra_info("port"),
                }

        return anyio.run(_inner)

    def is_authenticated(self) -> bool:
        """Check if authentication to the SFTP server is successful.

        This is a simplified version of test_connection() that returns
        a boolean indicating whether authentication succeeds.

        :return: True if authentication is successful, False otherwise
        :rtype: bool

        Example:
            Quick authentication check:

            .. code-block:: python

                sftp = SFTPResource(
                    host="sftp.example.com",
                    username="testuser",
                    password="testpass"
                )

                if sftp.is_authenticated():
                    # Proceed with operations
                    files = sftp.list_files("/data")
                else:
                    print("Authentication failed")
        """
        try:
            self.test_connection()
            return True
        except Exception:
            return False

    def get_file_info(
        self, remote_path: _SFTPPath, follow_symlinks: bool = True
    ) -> SFTPFileInfo:
        """Get detailed information about a specific file or directory.

        Retrieves comprehensive metadata about a file or directory on the SFTP server,
        including size, permissions, timestamps, and ownership information. This is
        useful for checking file properties before downloading or for monitoring
        file changes.

        :param remote_path: Path to the remote file or directory
        :type remote_path: str, Path, or bytes
        :param follow_symlinks: Whether to follow symbolic links to the target file or directory
        :type follow_symlinks: bool, optional
        :return: SFTPFileInfo object containing file metadata
        :rtype: SFTPFileInfo
        :raises asyncssh.SFTPNoSuchFile: If the file or directory doesn't exist
        :raises asyncssh.SFTPPermissionDenied: If lacking read permissions for the path
        :raises asyncssh.SFTPError: For other SFTP-related errors

        Example:
            Get file information:

            .. code-block:: python

                # Get file info
                file_info = sftp.get_file_info("/remote/data.csv")
                print(f"Size: {file_info.size} bytes")
                print(f"Modified: {file_info.mtime}")
                print(f"Is directory: {file_info.is_dir}")

                # Directory information
                dir_info = sftp.get_file_info("/remote/data_folder")
                if dir_info.is_dir:
                    print(f"Directory: {dir_info.name}")
                    # List contents
                    files = sftp.list_files("/remote/data_folder")
        """

        async def _inner() -> SFTPFileInfo:
            async with (
                asyncssh.connect(**self._connect_args) as conn,
                conn.start_sftp_client() as sftp,
            ):
                attrs = await sftp.stat(remote_path, follow_symlinks=follow_symlinks)

                # Convert path to string if it's bytes
                path_str = (
                    remote_path.decode()
                    if isinstance(remote_path, bytes)
                    else str(remote_path)
                )
                filename = Path(path_str).name
                return SFTPFileInfo.from_sftp_attrs(path_str, filename, attrs)

        return anyio.run(_inner)

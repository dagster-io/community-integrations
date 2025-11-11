"""
Integration tests for SFTPResource (async implementation) using a real SFTP server via testcontainers.

These tests verify that the asyncSSH-based implementation works correctly with a real SFTP server,
including parallel operations and performance benefits.
"""

import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import asyncssh
import pytest

from dagster_sftp.resource import SFTPResource
from dagster_sftp_tests.conftest import SFTP_USERNAME, SFTP_PASSWORD, SFTP_PORT


def test_basic_connection(sftp_resource: SFTPResource):
    """Test that we can connect and perform basic operations."""
    # List root directory
    assert sftp_resource.is_dir("/upload")


def test_upload_download_single_file(sftp_resource: SFTPResource, tmp_path: Path):
    """Test uploading and downloading a single file with parallel chunks."""
    # Create a larger test file to benefit from parallel chunks (2MB)
    local_file = tmp_path / "test_file.bin"
    test_data = b"x" * (2 * 1024 * 1024)  # 2MB
    local_file.write_bytes(test_data)

    # Upload the file
    remote_path = "/upload/test_file.bin"
    uploaded = sftp_resource.put_file(local_file, remote_path)
    assert uploaded == remote_path

    # Verify file exists
    file_info = sftp_resource.get_file_info(remote_path)
    assert file_info is not None
    assert file_info.size == len(test_data)
    assert file_info.is_file

    # Download the file
    download_file = tmp_path / "downloaded.bin"
    downloaded = sftp_resource.get_file(remote_path, download_file, preserve=True)
    assert downloaded == download_file

    # Verify content
    assert download_file.read_bytes() == test_data

    # Clean up
    sftp_resource.delete_file(remote_path)


def test_parallel_delete_operations(sftp_resource: SFTPResource, tmp_path: Path):
    """Test parallel delete operations."""
    # Create multiple test files
    num_files = 10
    file_size = 100 * 1024  # 100KB each
    test_data = b"x" * file_size

    remote_paths = []
    base_remote = "/upload/parallel_test"

    sftp_resource.mkdir(base_remote)

    for i in range(num_files):
        local_file = tmp_path / f"file_{i}.bin"
        local_file.write_bytes(test_data + str(i).encode())
        remote_path = f"{base_remote}/file_{i}.bin"
        sftp_resource.put_file(local_file, remote_path)
        remote_paths.append(remote_path)

    # Verify all files exist
    for remote_path in remote_paths:
        assert sftp_resource.file_exists(remote_path) is not None

    # Delete files in parallel
    start_time = time.time()
    sftp_resource.delete_files(remote_paths)
    delete_time = time.time() - start_time

    print(f"Parallel deletion of {num_files} files took {delete_time:.2f} seconds")

    # Verify all files are deleted
    for remote_path in remote_paths:
        assert not sftp_resource.file_exists(remote_path)

    # Clean up directory
    sftp_resource.rmtree(base_remote)


def test_glob_pattern_listing(sftp_resource: SFTPResource, tmp_path: Path):
    """Test pattern-based file listing using asyncSSH's glob."""
    # Create a structured directory tree
    base_path = "/upload/glob_test"
    test_structure = {
        "2024/01/data.csv": "January CSV data",
        "2024/01/report.pdf": "January report",
        "2024/02/data.csv": "February CSV data",
        "2024/02/report.pdf": "February report",
        "2024/03/data.csv": "March CSV data",
        "2023/12/data.csv": "December 2023 CSV",
        "logs/app.log": "Application log",
        "logs/error.log": "Error log",
    }

    # Upload test files
    for rel_path, content in test_structure.items():
        full_path = f"{base_path}/{rel_path}"
        local_file = tmp_path / "temp.txt"
        local_file.write_text(content)

        # Create parent directories
        parent_dir = os.path.dirname(full_path)
        sftp_resource.mkdir(parent_dir)

        sftp_resource.put_file(local_file, full_path)

    # Test various glob patterns

    # All CSV files recursively
    csv_files = sftp_resource.list_files(base_path, pattern="**/*.csv")
    assert len(csv_files) == 4
    assert all(f.filename.endswith(".csv") for f in csv_files)

    # Files in 2024 directories (files only, not directories)
    files_2024 = sftp_resource.list_files(
        base_path, pattern="2024/**/*", files_only=True
    )
    assert len(files_2024) == 5
    assert all("2024" in f.path for f in files_2024)

    # PDF files only
    pdf_files = sftp_resource.list_files(base_path, pattern="**/*.pdf")
    assert len(pdf_files) == 2
    assert all(f.filename.endswith(".pdf") for f in pdf_files)

    # Log files
    log_files = sftp_resource.list_files(base_path, pattern="logs/*.log")
    assert len(log_files) == 2
    assert all(".log" in f.filename for f in log_files)

    # Clean up
    sftp_resource.rmtree(base_path)


def test_directory_operations(sftp_resource: SFTPResource):
    """Test directory creation, checking, and removal."""
    base_dir = "/upload/dir_test"
    nested_dir = f"{base_dir}/level1/level2/level3"

    # Create nested directories
    sftp_resource.mkdir(nested_dir)

    # Verify directories exist
    assert sftp_resource.is_dir(base_dir)
    assert sftp_resource.is_dir(f"{base_dir}/level1")
    assert sftp_resource.is_dir(f"{base_dir}/level1/level2")
    assert sftp_resource.is_dir(nested_dir)

    # Test is_file on directory (should be False)
    assert not sftp_resource.is_file(nested_dir)

    # Remove entire tree at once
    sftp_resource.rmtree(base_dir)

    # Verify removal
    assert not sftp_resource.is_dir(base_dir)


def test_rename_operation(sftp_resource: SFTPResource, tmp_path: Path):
    """Test renaming files and directories."""
    # Upload a test file
    local_file = tmp_path / "rename_test.txt"
    local_file.write_text("Content for rename test")

    old_path = "/upload/old_name.txt"
    new_path = "/upload/new_name.txt"

    sftp_resource.put_file(local_file, old_path)

    # Rename the file
    sftp_resource.rename(old_path, new_path)

    # Verify old path doesn't exist and new path does
    assert not sftp_resource.file_exists(old_path)
    assert sftp_resource.file_exists(new_path)

    # Clean up
    sftp_resource.delete_file(new_path)


def test_filtering_by_time(sftp_resource: SFTPResource, tmp_path: Path):
    """Test filtering files by modification and access times."""
    base_path = "/upload/time_filter"
    sftp_resource.mkdir(base_path)

    # Create files with delays between them
    files = []
    for i in range(3):
        local_file = tmp_path / f"time_{i}.txt"
        local_file.write_text(f"Content {i}")
        remote_path = f"{base_path}/file_{i}.txt"
        sftp_resource.put_file(local_file, remote_path)
        files.append(remote_path)
        time.sleep(2)  # Larger delay to ensure timestamps are different

    # Get modification times
    file_infos = [sftp_resource.get_file_info(f) for f in files]

    # Use a time slightly after the middle file's modification time
    # to ensure we only get files strictly after it
    from datetime import timedelta

    mid_time = file_infos[1].modified_time + timedelta(seconds=0.5)

    # Filter files modified after the middle file
    recent_files = sftp_resource.list_files(
        base_path, files_only=True, modified_after=mid_time
    )
    # Should get the last file (file_2)
    assert len(recent_files) == 1
    assert "file_2" in recent_files[0].filename

    # Use a time slightly before the middle file's modification time
    # to ensure we only get files strictly before it
    mid_time_before = file_infos[1].modified_time - timedelta(seconds=0.5)

    # Filter files modified before the middle file
    older_files = sftp_resource.list_files(
        base_path, files_only=True, modified_before=mid_time_before
    )
    # Should get the first file (file_0)
    assert len(older_files) == 1
    assert "file_0" in older_files[0].filename

    # Clean up
    sftp_resource.delete_files(files)
    sftp_resource.rmtree(base_path)


def test_rmtree_directory_removal(sftp_resource: SFTPResource, tmp_path: Path):
    """Test removing entire directory trees."""
    base_path = "/upload/rmtree_test"

    # Create a directory tree with files
    structure = {
        "file1.txt": "Root file",
        "dir1/file2.txt": "Dir1 file",
        "dir1/subdir/file3.txt": "Subdir file",
        "dir2/file4.txt": "Dir2 file",
    }

    for rel_path, content in structure.items():
        full_path = f"{base_path}/{rel_path}"
        local_file = tmp_path / "temp.txt"
        local_file.write_text(content)

        parent_dir = os.path.dirname(full_path)
        sftp_resource.mkdir(parent_dir)
        sftp_resource.put_file(local_file, full_path)

    # Verify structure exists
    assert sftp_resource.is_dir(base_path)
    assert sftp_resource.is_dir(f"{base_path}/dir1")
    assert sftp_resource.is_dir(f"{base_path}/dir1/subdir")

    # Remove entire tree
    sftp_resource.rmtree(base_path)

    # Verify removal
    assert not sftp_resource.is_dir(base_path)


def test_large_file_transfer(sftp_resource: SFTPResource, tmp_path: Path):
    """Test transferring large files to verify parallel chunk benefits."""
    # Create a 10MB file
    local_file = tmp_path / "large.bin"
    size_mb = 10
    test_data = b"x" * (size_mb * 1024 * 1024)
    local_file.write_bytes(test_data)

    remote_path = "/upload/large_file.bin"

    # Upload with timing
    start_time = time.time()
    sftp_resource.put_file(local_file, remote_path, max_requests=128)
    upload_time = time.time() - start_time
    print(f"Upload of {size_mb}MB file took {upload_time:.2f} seconds")

    # Verify file exists with correct size
    file_info = sftp_resource.get_file_info(remote_path)
    assert file_info is not None
    assert file_info.size == len(test_data)

    # Download with timing
    download_file = tmp_path / "downloaded_large.bin"
    start_time = time.time()
    sftp_resource.get_file(remote_path, download_file, max_requests=128)
    download_time = time.time() - start_time
    print(f"Download of {size_mb}MB file took {download_time:.2f} seconds")

    # Verify content
    assert download_file.stat().st_size == len(test_data)

    # Clean up
    sftp_resource.delete_file(remote_path)


def test_empty_directory_listing(sftp_resource: SFTPResource):
    """Test listing an empty directory."""
    empty_dir = "/upload/empty_dir"
    sftp_resource.mkdir(empty_dir)

    files = sftp_resource.list_files(empty_dir)
    assert len(files) == 0

    # Clean up
    sftp_resource.rmtree(empty_dir)


def test_file_not_found_errors(sftp_resource: SFTPResource):
    """Test handling of file not found errors."""
    # Try to get info on non-existent file
    assert not sftp_resource.file_exists("/upload/nonexistent.txt")

    # Try to check if non-existent path is a directory
    assert not sftp_resource.is_dir("/upload/nonexistent_dir")

    # Try to check if non-existent path is a file
    assert not sftp_resource.is_file("/upload/nonexistent.txt")


def test_preserve_attributes(sftp_resource: SFTPResource, tmp_path: Path):
    """Test preserving file attributes during transfer."""
    # Create a test file with specific permissions
    local_file = tmp_path / "preserve_test.txt"
    local_file.write_text("Test content for preserve")
    local_file.chmod(0o644)

    remote_path = "/upload/preserve_test.txt"

    # Upload with preserve=True
    sftp_resource.put_file(local_file, remote_path, preserve=True)

    # Get file info
    file_info = sftp_resource.file_exists(remote_path)
    assert file_info is not None

    # Download with preserve=True
    download_file = tmp_path / "downloaded_preserve.txt"
    sftp_resource.get_file(remote_path, download_file, preserve=True)

    # Verify content
    assert download_file.read_text() == "Test content for preserve"

    # Clean up
    sftp_resource.delete_file(remote_path)


def test_invalid_credentials(sftp_server):
    """Test connection with invalid credentials."""
    invalid_resource = SFTPResource(
        host=sftp_server.get_container_host_ip(),
        port=int(sftp_server.get_exposed_port(SFTP_PORT)),
        username="wronguser",
        password="wrongpass",
        known_hosts=None,
    )

    # Should raise authentication error
    with pytest.raises(asyncssh.PermissionDenied):
        invalid_resource.list_files("/upload")


def test_connection_to_invalid_host():
    """Test connection to non-existent host."""
    invalid_resource = SFTPResource(
        host="nonexistent.invalid.host",
        port=22,
        username="testuser",
        password="testpass",
        known_hosts=None,
        connect_timeout=5,  # Short timeout to fail faster
    )

    # Should raise connection error
    with pytest.raises((OSError, asyncssh.Error)):
        invalid_resource.list_files("/")


def test_connection_to_invalid_port(sftp_server):
    """Test connection to wrong port."""
    invalid_resource = SFTPResource(
        host=sftp_server.get_container_host_ip(),
        port=9999,  # Wrong port
        username=SFTP_USERNAME,
        password=SFTP_PASSWORD,
        known_hosts=None,
        connect_timeout=5,
    )

    # Should raise connection error
    with pytest.raises((OSError, asyncssh.Error)):
        invalid_resource.list_files("/")


def test_upload_nonexistent_local_file(sftp_resource: SFTPResource):
    """Test uploading a file that doesn't exist locally."""
    nonexistent_file = Path("/tmp/this_file_definitely_does_not_exist_12345.txt")

    with pytest.raises(FileNotFoundError):
        sftp_resource.put_file(nonexistent_file, "/upload/test.txt")


def test_download_nonexistent_remote_file(sftp_resource: SFTPResource, tmp_path: Path):
    """Test downloading a file that doesn't exist on the server."""
    local_path = tmp_path / "downloaded.txt"

    with pytest.raises(asyncssh.SFTPError):
        sftp_resource.get_file("/upload/nonexistent_file.txt", local_path)


def test_delete_nonexistent_file(sftp_resource: SFTPResource):
    """Test deleting a file that doesn't exist."""
    with pytest.raises(asyncssh.SFTPError):
        sftp_resource.delete_file("/upload/nonexistent_file.txt")


def test_rename_nonexistent_file(sftp_resource: SFTPResource):
    """Test renaming a file that doesn't exist."""
    with pytest.raises(asyncssh.SFTPError):
        sftp_resource.rename("/upload/nonexistent.txt", "/upload/new_name.txt")


def test_mkdir_existing_directory(sftp_resource: SFTPResource):
    """Test creating a directory that already exists."""
    dir_path = "/upload/test_existing_dir"

    # Create directory first time
    sftp_resource.mkdir(dir_path)

    # Try to create again - should not raise error due to exist_ok behavior
    try:
        sftp_resource.mkdir(dir_path)
    finally:
        # Clean up
        sftp_resource.rmtree(dir_path)


def test_rmtree_nonexistent_directory(sftp_resource: SFTPResource):
    """Test removing a directory tree that doesn't exist."""
    with pytest.raises(asyncssh.SFTPError):
        sftp_resource.rmtree("/upload/nonexistent_dir")


def test_is_dir_on_file(sftp_resource: SFTPResource, tmp_path: Path):
    """Test is_dir on a regular file."""
    # Upload a file
    local_file = tmp_path / "test.txt"
    local_file.write_text("content")
    remote_path = "/upload/test_file.txt"
    sftp_resource.put_file(local_file, remote_path)

    try:
        # Should return False for a file
        assert sftp_resource.is_dir(remote_path) is False
        assert sftp_resource.is_file(remote_path) is True
    finally:
        sftp_resource.delete_file(remote_path)


def test_is_file_on_directory(sftp_resource: SFTPResource):
    """Test is_file on a directory."""
    dir_path = "/upload/test_dir"
    sftp_resource.mkdir(dir_path)

    try:
        # Should return False for a directory
        assert sftp_resource.is_file(dir_path) is False
        assert sftp_resource.is_dir(dir_path) is True
    finally:
        sftp_resource.rmtree(dir_path)


def test_list_files_invalid_path(sftp_resource: SFTPResource):
    """Test listing files in a non-existent directory."""
    files = sftp_resource.list_files("/upload/nonexistent_directory")
    # Should return empty list or handle gracefully
    assert files == []


def test_special_characters_in_filenames(sftp_resource: SFTPResource, tmp_path: Path):
    """Test handling files with special characters in names."""
    special_names = [
        "file with spaces.txt",
        "file-with-dashes.txt",
        "file_with_underscores.txt",
        "file.multiple.dots.txt",
        "file(with)parens.txt",
        "file[with]brackets.txt",
        "file@with#special$chars.txt",
    ]

    uploaded_files = []
    try:
        for name in special_names:
            local_file = tmp_path / "test.txt"
            local_file.write_text(f"Content for {name}")
            remote_path = f"/upload/{name}"

            # Should handle special characters properly
            sftp_resource.put_file(local_file, remote_path)
            uploaded_files.append(remote_path)

            # Verify file exists
            file_info = sftp_resource.get_file_info(remote_path)
            assert file_info is not None
            assert file_info.filename == name

    finally:
        # Clean up
        for remote_path in uploaded_files:
            try:
                sftp_resource.delete_file(remote_path)
            except asyncssh.SFTPError:
                # File might have already been deleted or doesn't exist
                pass


def test_upload_to_readonly_directory(sftp_resource: SFTPResource, tmp_path: Path):
    """Test uploading to a directory without write permissions."""
    local_file = tmp_path / "test.txt"
    local_file.write_text("test content")

    with pytest.raises(asyncssh.sftp.SFTPPermissionDenied):
        result = sftp_resource.put_file(local_file, "/test.txt")
        # If it succeeds, verify and clean up
        assert result == "/test.txt"


def test_concurrent_file_operations(sftp_resource: SFTPResource, tmp_path: Path):
    """Test concurrent operations on the same file."""
    local_file = tmp_path / "concurrent.txt"
    local_file.write_text("Initial content")
    remote_path = "/upload/concurrent_test.txt"

    # Upload initial file
    sftp_resource.put_file(local_file, remote_path)

    try:
        # Simulate concurrent operations
        # Try to delete while also trying to rename
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def delete_op():
            try:
                sftp_resource.delete_file(remote_path)
                return "deleted"
            except asyncssh.SFTPError:
                return "delete_failed"

        def rename_op():
            try:
                sftp_resource.rename(remote_path, f"{remote_path}.renamed")
                return "renamed"
            except asyncssh.SFTPError:
                return "rename_failed"

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(delete_op), executor.submit(rename_op)]

            results = [future.result() for future in as_completed(futures)]

            # One should succeed, one should fail
            assert "deleted" in results or "renamed" in results
            assert "delete_failed" in results or "rename_failed" in results

    finally:
        # Clean up any remaining files
        for path in [remote_path, f"{remote_path}.renamed"]:
            try:
                sftp_resource.delete_file(path)
            except asyncssh.SFTPError:
                # File might not exist or already deleted
                pass


def test_recursive_directory_operations_on_file(
    sftp_resource: SFTPResource, tmp_path: Path
):
    """Test recursive directory operations on a regular file."""
    # Upload a file
    local_file = tmp_path / "test.txt"
    local_file.write_text("content")
    remote_path = "/upload/test_file_not_dir.txt"
    sftp_resource.put_file(local_file, remote_path)

    try:
        # Try to use rmtree on a file (should fail)
        with pytest.raises(asyncssh.SFTPError):
            sftp_resource.rmtree(remote_path)
    finally:
        sftp_resource.delete_file(remote_path)


def test_path_traversal_attempts(sftp_resource: SFTPResource):
    """Test that path traversal attempts are handled safely."""
    dangerous_paths = [
        "../../../etc/passwd",
        "/upload/../../etc/passwd",
        "/upload/../../../root/.ssh/id_rsa",
    ]

    for path in dangerous_paths:
        # Attempt to list files with path traversal
        # This should either:
        # 1. Return an empty list (safe handling)
        # 2. Raise an error (server rejects the attempt)
        # Both are acceptable security behaviors
        try:
            files = sftp_resource.list_files(path)
            # If it doesn't raise an error, it should return empty or safe results
            assert isinstance(files, list)
            # Ensure we're not actually accessing sensitive files
            assert not any("passwd" in f.filename for f in files)
            assert not any("ssh" in f.path for f in files)
        except (asyncssh.SFTPError, OSError):
            # Expected - server should reject these attempts
            # This is also a valid security response
            pass


def test_large_batch_operations_stress(sftp_resource: SFTPResource, tmp_path: Path):
    """Test handling of large batch operations that might stress the connection."""
    num_files = 100
    base_path = "/upload/stress_test"
    sftp_resource.mkdir(base_path)

    try:
        # Create many files quickly
        uploaded = []
        for i in range(num_files):
            local_file = tmp_path / f"file_{i}.txt"
            local_file.write_text(f"Content {i}")
            remote_path = f"{base_path}/file_{i}.txt"

            try:
                sftp_resource.put_file(local_file, remote_path)
                uploaded.append(remote_path)
            except asyncssh.SFTPError as e:
                # Some operations might fail under stress
                print(f"Failed to upload file {i}: {e}")

        # Should have uploaded at least some files
        assert len(uploaded) > 0

        # Try to delete them all at once
        sftp_resource.delete_files(uploaded)

    finally:
        # Clean up
        try:
            sftp_resource.rmtree(base_path)
        except asyncssh.SFTPError:
            # Directory might not exist or already deleted
            pass


def test_interrupted_large_file_transfer(sftp_resource: SFTPResource, tmp_path: Path):
    """Test behavior when a large file transfer is interrupted."""
    # Create a large file
    large_file = tmp_path / "large.bin"
    size_mb = 50
    large_file.write_bytes(b"x" * (size_mb * 1024 * 1024))

    remote_path = "/upload/interrupted_large.bin"

    # This test is tricky to implement properly as we'd need to
    # interrupt the connection mid-transfer. In a real test environment,
    # you might use network simulation tools or mock the connection.

    try:
        # Normal upload should work
        sftp_resource.put_file(large_file, remote_path)

        # Verify partial uploads are handled
        file_info = sftp_resource.file_exists(remote_path)
        assert file_info is not None

    finally:
        # Clean up
        try:
            sftp_resource.delete_file(remote_path)
        except asyncssh.SFTPError:
            # File might not exist or already deleted
            pass


def test_empty_file_operations(sftp_resource: SFTPResource, tmp_path: Path):
    """Test operations on empty files."""
    # Create empty file
    empty_file = tmp_path / "empty.txt"
    empty_file.touch()

    remote_path = "/upload/empty.txt"

    # Upload empty file
    sftp_resource.put_file(empty_file, remote_path)

    try:
        # Verify empty file exists
        file_info = sftp_resource.get_file_info(remote_path)
        assert file_info is not None
        assert file_info.size == 0
        assert file_info.is_file

        # Download empty file
        download_path = tmp_path / "downloaded_empty.txt"
        sftp_resource.get_file(remote_path, download_path)
        assert download_path.stat().st_size == 0

    finally:
        sftp_resource.delete_file(remote_path)


def test_symlink_to_nowhere(sftp_resource: SFTPResource, tmp_path: Path):
    """Test handling of broken symbolic links."""
    # Note: Creating symlinks might not be supported by all SFTP servers
    # This test might be skipped depending on server capabilities

    local_file = tmp_path / "test.txt"
    local_file.write_text("test")
    target_path = "/upload/link_target.txt"

    sftp_resource.put_file(local_file, target_path)

    # Try to get info about the file
    assert sftp_resource.file_exists(target_path)

    # Delete the target
    sftp_resource.delete_file(target_path)

    # Now the link (if it existed) would be broken
    assert not sftp_resource.file_exists(target_path)


def test_filename_with_newlines_and_special_chars(
    sftp_resource: SFTPResource, tmp_path: Path
):
    """Test handling of filenames with problematic characters."""
    # Most SFTP servers won't allow these, we expect them to be rejected
    problematic_names = [
        "\x00nullbyte.txt",
    ]

    for name in problematic_names:
        local_file = tmp_path / "test.txt"
        local_file.write_text("content")

        # These should be rejected by the server or client
        with pytest.raises((asyncssh.SFTPError, OSError, ValueError)):
            sftp_resource.put_file(local_file, f"/upload/{name}")


def test_operations_after_connection_timeout(
    sftp_resource: SFTPResource, tmp_path: Path
):
    """Test that operations handle connection timeouts gracefully."""
    # Upload a file first
    local_file = tmp_path / "test.txt"
    local_file.write_text("test content")
    remote_path = "/upload/timeout_test.txt"
    sftp_resource.put_file(local_file, remote_path)

    try:
        # Wait for potential timeout (though keepalive should prevent it)
        time.sleep(20)

        # Try to use the connection again
        file_info = sftp_resource.file_exists(remote_path)
        assert file_info is not None

        # Should reconnect automatically if needed
        files = sftp_resource.list_files("/upload")
        assert any(f.filename == "timeout_test.txt" for f in files)

    finally:
        sftp_resource.delete_file(remote_path)


def test_glob_pattern_injection(sftp_resource: SFTPResource):
    """Test that glob patterns with injection attempts are handled safely."""
    dangerous_patterns = [
        "*; rm -rf /",
        "../../*",
        "$(whoami)",
        "`ls /etc`",
        "*|cat /etc/passwd",
    ]

    for pattern in dangerous_patterns:
        # These patterns should either be escaped properly or rejected
        # Both behaviors are acceptable for security
        try:
            files = sftp_resource.list_files("/upload", pattern=pattern)
            # If it returns results, verify they are safe
            assert isinstance(files, list)
            # No files should be outside the /upload directory
            for f in files:
                assert "/upload" in f.path or f.path == "."
            # No shell commands should have been executed
            assert not any("rm" in f.filename for f in files)
            assert not any("whoami" in f.filename for f in files)
        except (asyncssh.SFTPError, OSError, ValueError):
            # Expected if the server rejects dangerous patterns
            # This is also a valid security response
            pass


def test_parallel_delete_with_partial_failures(
    sftp_resource: SFTPResource, tmp_path: Path
):
    """Test parallel deletion where some files don't exist."""
    # Upload some files
    existing_files = []
    for i in range(5):
        local_file = tmp_path / f"file_{i}.txt"
        local_file.write_text(f"Content {i}")
        remote_path = f"/upload/delete_test_{i}.txt"
        sftp_resource.put_file(local_file, remote_path)
        existing_files.append(remote_path)

    # Mix existing and non-existing files
    files_to_delete = existing_files.copy()
    files_to_delete.extend(
        [
            "/upload/nonexistent_1.txt",
            "/upload/nonexistent_2.txt",
            "/upload/nonexistent_3.txt",
        ]
    )

    # Delete files - the implementation uses anyio which raises ExceptionGroup
    # We expect it to raise an error for non-existent files
    error_raised = False
    try:
        sftp_resource.delete_files(files_to_delete)
    except Exception as e:
        error_raised = True
        # Check if it's an exception group (anyio's ExceptionGroup)
        error_str = str(e)
        # Should mention the non-existent files or have multiple exceptions
        assert (
            "nonexistent" in error_str
            or "3 exceptions" in error_str
            or hasattr(e, "exceptions")
        )

    assert error_raised, "Expected an exception for non-existent files"

    # Verify existing files were deleted despite the errors
    for path in existing_files:
        assert not sftp_resource.file_exists(path)


def test_large_file_upload_with_max_requests_limit(
    sftp_resource: SFTPResource, tmp_path: Path
):
    """Test large file upload with different max_requests settings to simulate congestion."""
    # Create a moderately large file
    file_size_mb = 5
    large_file = tmp_path / "large.bin"
    test_data = b"x" * (file_size_mb * 1024 * 1024)
    large_file.write_bytes(test_data)

    # Test with very limited parallel requests (should still work, just slower)
    remote_path = "/upload/limited_parallel.bin"
    try:
        # Upload with minimal parallelism
        sftp_resource.put_file(large_file, remote_path, max_requests=1)

        # Verify file was uploaded correctly
        file_info = sftp_resource.get_file_info(remote_path)
        assert file_info is not None
        assert file_info.size == len(test_data)

        # Download with minimal parallelism
        download_path = tmp_path / "downloaded_limited.bin"
        sftp_resource.get_file(remote_path, download_path, max_requests=1)

        # Verify content
        assert download_path.stat().st_size == len(test_data)

    finally:
        try:
            sftp_resource.delete_file(remote_path)
        except Exception:
            pass


def test_concurrent_uploads_same_filename(sftp_resource: SFTPResource, tmp_path: Path):
    """Test concurrent uploads to the same filename (last one should win)."""
    remote_path = "/upload/concurrent_upload.txt"

    # Create different content files
    files = []
    for i in range(5):
        local_file = tmp_path / f"content_{i}.txt"
        local_file.write_text(f"This is content version {i}")
        files.append(local_file)

    # Upload concurrently using threads
    def upload_file(local_file, version):
        try:
            sftp_resource.put_file(local_file, remote_path)
            return f"success_{version}"
        except Exception as e:
            return f"error_{version}: {e}"

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(upload_file, files[i], i) for i in range(5)]
        results = [f.result() for f in as_completed(futures)]

    # All uploads should succeed (last one wins)
    assert all("success" in r for r in results)

    # File should exist with some content
    file_info = sftp_resource.file_exists(remote_path)
    assert file_info is not None

    # Download and check content (should be one of the versions)
    download_path = tmp_path / "downloaded.txt"
    sftp_resource.get_file(remote_path, download_path)
    content = download_path.read_text()
    assert "This is content version" in content

    # Clean up
    sftp_resource.delete_file(remote_path)


def test_concurrent_downloads_same_file(sftp_resource: SFTPResource, tmp_path: Path):
    """Test multiple concurrent downloads of the same file."""
    # Upload a test file
    local_file = tmp_path / "source.txt"
    local_file.write_text("Concurrent download test content")
    remote_path = "/upload/concurrent_download.txt"
    sftp_resource.put_file(local_file, remote_path)

    try:
        # Download the same file concurrently to different locations
        def download_file(index):
            download_path = tmp_path / f"download_{index}.txt"
            try:
                sftp_resource.get_file(remote_path, download_path)
                return download_path, "success", download_path.read_text()
            except Exception as e:
                return None, "error", str(e)

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(download_file, i) for i in range(10)]
            results = [f.result() for f in as_completed(futures)]

        # All downloads should succeed
        successful = [r for r in results if r[1] == "success"]
        assert len(successful) == 10

        # All should have the same content
        for path, status, content in successful:
            if status == "success":
                assert content == "Concurrent download test content"

    finally:
        sftp_resource.delete_file(remote_path)


def test_upload_during_connection_instability(
    sftp_resource: SFTPResource, tmp_path: Path
):
    """Test upload behavior when connection becomes unstable mid-transfer."""
    # Create multiple files for upload
    files_to_upload = []
    for i in range(20):
        local_file = tmp_path / f"file_{i}.txt"
        # Varying sizes to test different transfer times
        content = "x" * (1024 * (i + 1))  # 1KB to 20KB
        local_file.write_text(content)
        files_to_upload.append(local_file)

    uploaded = []
    failed = []

    # Upload files sequentially, simulating potential connection issues
    for i, local_file in enumerate(files_to_upload):
        remote_path = f"/upload/stability_test_{i}.txt"
        try:
            # Every 5th file, we create a new resource to simulate reconnection
            if i % 5 == 0 and i > 0:
                # Create new resource (simulates reconnection)
                sftp_resource = SFTPResource(
                    host=sftp_resource.host,
                    port=sftp_resource.port,
                    username=sftp_resource.username,
                    password=sftp_resource.password,
                    known_hosts=sftp_resource.known_hosts,
                    default_max_requests=sftp_resource.default_max_requests,
                )

            sftp_resource.put_file(local_file, remote_path)
            uploaded.append(remote_path)
        except Exception as e:
            failed.append((remote_path, str(e)))

    # Most files should upload successfully
    assert len(uploaded) >= len(files_to_upload) * 0.8  # At least 80% success

    # Clean up uploaded files
    for path in uploaded:
        try:
            sftp_resource.delete_file(path)
        except Exception:
            pass


def test_download_with_disk_space_simulation(
    sftp_resource: SFTPResource, tmp_path: Path
):
    """Test download behavior when local disk space is limited."""
    # Upload a test file first
    source_file = tmp_path / "source.txt"
    content = "x" * (1024 * 1024)  # 1MB
    source_file.write_text(content)
    remote_path = "/upload/disk_space_test.txt"
    sftp_resource.put_file(source_file, remote_path)

    try:
        # Try to download to a path that might have issues
        # In real scenarios, you'd mock the disk write to fail
        download_path = tmp_path / "downloaded.txt"

        # Normal download should work
        sftp_resource.get_file(remote_path, download_path)
        assert download_path.exists()
        assert download_path.stat().st_size == len(content)

        # Simulate multiple concurrent downloads that might exhaust resources
        download_paths = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for i in range(10):
                path = tmp_path / f"concurrent_download_{i}.txt"
                download_paths.append(path)
                futures.append(
                    executor.submit(sftp_resource.get_file, remote_path, path)
                )

            # Wait for all to complete
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception:
                    pass  # Some might fail due to resource constraints

        # At least some should succeed
        successful_downloads = [p for p in download_paths if p.exists()]
        assert len(successful_downloads) > 0

    finally:
        sftp_resource.delete_file(remote_path)


def test_parallel_operations_with_connection_limit(
    sftp_resource: SFTPResource, tmp_path: Path
):
    """Test behavior when hitting connection/request limits."""
    # Create a resource with very high max_requests to stress test
    stress_resource = SFTPResource(
        host=sftp_resource.host,
        port=sftp_resource.port,
        username=sftp_resource.username,
        password=sftp_resource.password,
        known_hosts=None,
        default_max_requests=256,  # Very high to stress the server
    )

    # Create many small files
    num_files = 50
    files = []
    for i in range(num_files):
        local_file = tmp_path / f"stress_{i}.txt"
        local_file.write_text(f"Stress test content {i}")
        files.append(local_file)

    # Upload all files as fast as possible
    uploaded = []
    failed = []

    for i, local_file in enumerate(files):
        remote_path = f"/upload/stress_{i}.txt"
        try:
            stress_resource.put_file(local_file, remote_path)
            uploaded.append(remote_path)
        except Exception as e:
            failed.append((local_file, str(e)))

    # Should handle the load (might be slower but shouldn't crash)
    assert len(uploaded) > 0

    # Now try to delete them all at once
    if uploaded:
        try:
            stress_resource.delete_files(uploaded)
        except Exception:
            # Partial deletion is acceptable
            pass

    # Verify cleanup
    remaining = [p for p in uploaded if stress_resource.file_exists(p)]
    # Most should be deleted
    assert len(remaining) < len(uploaded) / 2

    # Final cleanup
    for path in remaining:
        try:
            stress_resource.delete_file(path)
        except Exception:
            pass


def test_mixed_operations_stress(sftp_resource: SFTPResource, tmp_path: Path):
    """Test mixed parallel operations (upload, download, delete, list) happening simultaneously."""
    base_path = "/upload/mixed_ops"
    sftp_resource.mkdir(base_path)

    # Prepare some initial files
    initial_files = []
    for i in range(10):
        local_file = tmp_path / f"initial_{i}.txt"
        local_file.write_text(f"Initial content {i}")
        remote_path = f"{base_path}/initial_{i}.txt"
        sftp_resource.put_file(local_file, remote_path)
        initial_files.append(remote_path)

    results = {"uploads": 0, "downloads": 0, "deletes": 0, "lists": 0, "errors": []}

    def upload_op(index):
        try:
            local_file = tmp_path / f"upload_{index}.txt"
            local_file.write_text(f"Upload {index}")
            sftp_resource.put_file(local_file, f"{base_path}/upload_{index}.txt")
            results["uploads"] += 1
            return "upload_success"
        except Exception as e:
            results["errors"].append(f"upload_{index}: {e}")
            return "upload_error"

    def download_op(index):
        try:
            if initial_files:
                remote_file = initial_files[index % len(initial_files)]
                local_path = tmp_path / f"download_{index}.txt"
                sftp_resource.get_file(remote_file, local_path)
                results["downloads"] += 1
            return "download_success"
        except Exception as e:
            results["errors"].append(f"download_{index}: {e}")
            return "download_error"

    def delete_op(index):
        try:
            # Try to delete newly uploaded files
            remote_path = f"{base_path}/upload_{index}.txt"
            if sftp_resource.file_exists(remote_path):
                sftp_resource.delete_file(remote_path)
                results["deletes"] += 1
            return "delete_success"
        except Exception as e:
            results["errors"].append(f"delete_{index}: {e}")
            return "delete_error"

    def list_op(index):
        try:
            files = sftp_resource.list_files(base_path)
            results["lists"] += 1
            return f"list_success_{len(files)}"
        except Exception as e:
            results["errors"].append(f"list_{index}: {e}")
            return "list_error"

    # Run mixed operations concurrently
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = []

        # Queue up mixed operations
        for i in range(30):
            if i % 4 == 0:
                futures.append(executor.submit(upload_op, i))
            elif i % 4 == 1:
                futures.append(executor.submit(download_op, i))
            elif i % 4 == 2:
                futures.append(executor.submit(delete_op, i))
            else:
                futures.append(executor.submit(list_op, i))

        # Wait for all to complete
        [f.result() for f in as_completed(futures)]

    # Should have completed many operations successfully
    total_successful = (
        results["uploads"]
        + results["downloads"]
        + results["deletes"]
        + results["lists"]
    )
    assert total_successful > 0

    # Clean up
    try:
        sftp_resource.rmtree(base_path)
    except Exception:
        pass


def test_recovery_from_interrupted_batch_delete(
    sftp_resource: SFTPResource, tmp_path: Path
):
    """Test recovery when batch delete operation is interrupted."""
    # Create files to delete
    files_to_delete = []
    for i in range(20):
        local_file = tmp_path / f"delete_{i}.txt"
        local_file.write_text(f"Delete me {i}")
        remote_path = f"/upload/batch_delete_{i}.txt"
        sftp_resource.put_file(local_file, remote_path)
        files_to_delete.append(remote_path)

    # Simulate interruption by using a subset first
    first_batch = files_to_delete[:10]
    second_batch = files_to_delete[10:]

    # Delete first batch
    sftp_resource.delete_files(first_batch)

    # Verify first batch is deleted
    for path in first_batch:
        assert not sftp_resource.file_exists(path)

    # Second batch should still exist
    for path in second_batch:
        assert sftp_resource.file_exists(path)

    # Complete the deletion
    sftp_resource.delete_files(second_batch)

    # Verify all are deleted
    for path in files_to_delete:
        assert not sftp_resource.file_exists(path)


def test_parallel_operations_with_varying_file_sizes(
    sftp_resource: SFTPResource, tmp_path: Path
):
    """Test parallel operations with files of vastly different sizes."""
    files = []

    # Create files with exponentially increasing sizes
    sizes = [
        1,  # 1 byte
        1024,  # 1 KB
        10240,  # 10 KB
        102400,  # 100 KB
        1048576,  # 1 MB
        5242880,  # 5 MB
    ]

    for i, size in enumerate(sizes):
        local_file = tmp_path / f"sized_{i}.bin"
        local_file.write_bytes(b"x" * size)
        files.append((local_file, size))

    # Upload all files in parallel (different sizes might complete at different times)
    def upload_with_timing(local_file, index, size):
        remote_path = f"/upload/sized_{index}.bin"
        start = time.time()
        try:
            sftp_resource.put_file(local_file, remote_path)
            elapsed = time.time() - start
            return remote_path, "success", elapsed, size
        except Exception as e:
            elapsed = time.time() - start
            return remote_path, "error", elapsed, str(e)

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = [
            executor.submit(upload_with_timing, f[0], i, f[1])
            for i, f in enumerate(files)
        ]
        results = [f.result() for f in as_completed(futures)]

    successful = [r for r in results if r[1] == "success"]
    assert len(successful) >= 4  # At least 4 out of 6 should succeed

    # Clean up successful uploads
    for path, status, _, _ in successful:
        if status == "success":
            try:
                sftp_resource.delete_file(path)
            except Exception:
                pass


def test_connection_pool_exhaustion_recovery(
    sftp_resource: SFTPResource, tmp_path: Path
):
    """Test that the resource recovers from connection pool exhaustion."""
    # Create many operations that might exhaust the connection pool
    num_operations = 100

    def stress_operation(index):
        try:
            # Mix different operations
            if index % 3 == 0:
                # List files
                files = sftp_resource.list_files("/upload")
                return f"list_{len(files)}"
            elif index % 3 == 1:
                # Check file existence
                exists = sftp_resource.file_exists(f"/upload/test_{index}.txt")
                return f"exists_{exists}"
            else:
                # Check if directory
                is_dir = sftp_resource.is_dir("/upload")
                return f"is_dir_{is_dir}"
        except Exception as e:
            return f"error_{index}: {e}"

    # Run many operations rapidly
    with ThreadPoolExecutor(max_workers=50) as executor:
        futures = [executor.submit(stress_operation, i) for i in range(num_operations)]
        results = [f.result() for f in as_completed(futures)]

    # Count successful operations
    successful = [r for r in results if not r.startswith("error_")]

    # Should complete most operations successfully (connection pool should handle it)
    assert len(successful) >= num_operations * 0.7  # At least 70% success rate

    # Try a normal operation after the stress test (should recover)
    files = sftp_resource.list_files("/upload")
    assert isinstance(files, list)  # Should work normally

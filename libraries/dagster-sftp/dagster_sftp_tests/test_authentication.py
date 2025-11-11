"""
Tests for authentication checking methods in SFTPResource.
"""

from pathlib import Path

import asyncssh
import pytest
from dagster_sftp.resource import SFTPResource
from dagster_sftp_tests.conftest import SFTP_USERNAME, SFTP_PASSWORD, SFTP_PORT


def test_connection_successful_password_auth(sftp_server):
    """Test that test_connection returns correct info for successful password auth."""
    _sftp_resource = SFTPResource(
        host=sftp_server.get_container_host_ip(),
        port=int(sftp_server.get_exposed_port(SFTP_PORT)),
        username=SFTP_USERNAME,
        password=SFTP_PASSWORD,
        known_hosts=None,
    )

    # Test the connection - should not raise
    info = _sftp_resource.test_connection()

    # Verify successful connection info
    assert info["authenticated_user"] == SFTP_USERNAME
    assert "server_version" in info
    assert "client_version" in info
    assert info["host"] == sftp_server.get_container_host_ip()
    assert info["port"] == int(sftp_server.get_exposed_port(SFTP_PORT))


def test_connection_failed_wrong_details(sftp_server):
    """Test that test_connection raises error for wrong port."""
    _sftp_resource = SFTPResource(
        host=sftp_server.get_container_host_ip(),
        port=9999,  # Wrong port
        username=SFTP_USERNAME,
        password=SFTP_PASSWORD,
        known_hosts=None,
        connect_timeout=2,  # Short timeout
    )

    # Test the connection - should raise an error
    with pytest.raises((OSError, asyncssh.Error)):
        _sftp_resource.test_connection()


def test_is_authenticated_success(sftp_server):
    """Test that is_authenticated returns True for valid credentials."""
    _sftp_resource = SFTPResource(
        host=sftp_server.get_container_host_ip(),
        port=int(sftp_server.get_exposed_port(SFTP_PORT)),
        username=SFTP_USERNAME,
        password=SFTP_PASSWORD,
        known_hosts=None,
    )

    # Check authentication
    assert _sftp_resource.is_authenticated() is True


def test_is_authenticated_failure(sftp_server):
    """Test that is_authenticated returns False for invalid credentials."""
    _sftp_resource = SFTPResource(
        host=sftp_server.get_container_host_ip(),
        port=int(sftp_server.get_exposed_port(SFTP_PORT)),
        username=SFTP_USERNAME,
        password="wrong_password",
        known_hosts=None,
    )

    # Check authentication
    assert _sftp_resource.is_authenticated() is False


def test_connection_info_completeness(sftp_server):
    """Test that test_connection returns all expected information fields."""
    _sftp_resource = SFTPResource(
        host=sftp_server.get_container_host_ip(),
        port=int(sftp_server.get_exposed_port(SFTP_PORT)),
        username=SFTP_USERNAME,
        password=SFTP_PASSWORD,
        known_hosts=None,
    )

    info = _sftp_resource.test_connection()

    # Check all expected fields are present for successful connection
    expected_fields = [
        "authenticated_user",
        "server_version",
        "client_version",
        "host",
        "port",
    ]

    for field in expected_fields:
        assert field in info, f"Missing field: {field}"

    # Verify types
    assert isinstance(info["authenticated_user"], str)


def test_sftp_auth_with_private_key_file_path(
    sftp_server_with_ssh_key, ssh_keys, tmp_path: Path
):
    """Test authentication using a private key file path."""
    private_key, public_key = ssh_keys

    # Write private key to a file
    private_key_file = tmp_path / "test_private_key"
    private_key_file.write_text(private_key)
    private_key_file.chmod(0o600)  # Set proper permissions for SSH key

    # Create SFTP resource with private key file path
    sftp_resource = SFTPResource(
        host=sftp_server_with_ssh_key.get_container_host_ip(),
        port=int(sftp_server_with_ssh_key.get_exposed_port(SFTP_PORT)),
        username=SFTP_USERNAME,
        private_key_path=str(private_key_file),
        known_hosts=None,
    )

    # Test basic connection and operations
    # Create a test file
    local_test_file = tmp_path / "test.txt"
    local_test_file.write_text("Hello from SSH key auth test!")

    # Upload file
    remote_path = "/upload/ssh_key_test.txt"
    uploaded = sftp_resource.put_file(local_test_file, remote_path)
    assert uploaded == remote_path

    # Verify file exists
    assert sftp_resource.file_exists(remote_path)

    # Download file
    download_file = tmp_path / "downloaded.txt"
    downloaded = sftp_resource.get_file(remote_path, download_file)
    assert downloaded == download_file

    # Verify content
    assert download_file.read_text() == "Hello from SSH key auth test!"

    # List files
    files = sftp_resource.list_files("/upload")
    assert any(f.filename == "ssh_key_test.txt" for f in files)

    # Clean up
    sftp_resource.delete_file(remote_path)
    assert not sftp_resource.file_exists(remote_path)


def test_sftp_auth_with_private_key_string(
    sftp_server_with_ssh_key, ssh_keys, tmp_path: Path
):
    """Test authentication using a private key provided as a string."""
    private_key, public_key = ssh_keys

    # Create SFTP resource with private key as string
    sftp_resource = SFTPResource(
        host=sftp_server_with_ssh_key.get_container_host_ip(),
        port=int(sftp_server_with_ssh_key.get_exposed_port(SFTP_PORT)),
        username=SFTP_USERNAME,
        private_key_content=private_key,  # Pass the key content directly
        known_hosts=None,
    )

    # Test basic connection and operations
    # Create a test file
    local_test_file = tmp_path / "test_string.txt"
    local_test_file.write_text("Hello from SSH key string auth test!")

    # Upload file
    remote_path = "/upload/ssh_key_string_test.txt"
    uploaded = sftp_resource.put_file(local_test_file, remote_path)
    assert uploaded == remote_path

    # Verify file exists
    assert sftp_resource.file_exists(remote_path)

    # Download file
    download_file = tmp_path / "downloaded_string.txt"
    downloaded = sftp_resource.get_file(remote_path, download_file)
    assert downloaded == download_file

    # Verify content
    assert download_file.read_text() == "Hello from SSH key string auth test!"

    # List files
    files = sftp_resource.list_files("/upload")
    assert any(f.filename == "ssh_key_string_test.txt" for f in files)

    # Clean up
    sftp_resource.delete_file(remote_path)
    assert not sftp_resource.file_exists(remote_path)


def test_sftp_auth_with_private_key_file_path_and_passphrase(
    sftp_server_with_ssh_key_passphrase, ssh_keys_with_passphrase, tmp_path: Path
):
    """Test authentication using a private key file path with passphrase."""
    private_key, public_key, passphrase = ssh_keys_with_passphrase

    # Write private key to a file
    private_key_file = tmp_path / "test_private_key_with_pass"
    private_key_file.write_text(private_key)
    private_key_file.chmod(0o600)  # Set proper permissions for SSH key

    # Create SFTP resource with private key file path and passphrase
    sftp_resource = SFTPResource(
        host=sftp_server_with_ssh_key_passphrase.get_container_host_ip(),
        port=int(sftp_server_with_ssh_key_passphrase.get_exposed_port(SFTP_PORT)),
        username=SFTP_USERNAME,
        private_key_path=str(private_key_file),
        passphrase=passphrase,
        known_hosts=None,
    )

    # Test basic connection and operations
    # Create a test file
    local_test_file = tmp_path / "test_passphrase.txt"
    local_test_file.write_text("Hello from SSH key with passphrase test!")

    # Upload file
    remote_path = "/upload/ssh_key_passphrase_test.txt"
    uploaded = sftp_resource.put_file(local_test_file, remote_path)
    assert uploaded == remote_path

    # Verify file exists
    assert sftp_resource.file_exists(remote_path)

    # Download file
    download_file = tmp_path / "downloaded_passphrase.txt"
    downloaded = sftp_resource.get_file(remote_path, download_file)
    assert downloaded == download_file

    # Verify content
    assert download_file.read_text() == "Hello from SSH key with passphrase test!"

    # Clean up
    sftp_resource.delete_file(remote_path)
    assert not sftp_resource.file_exists(remote_path)


def test_sftp_auth_with_private_key_string_and_passphrase(
    sftp_server_with_ssh_key_passphrase, ssh_keys_with_passphrase, tmp_path: Path
):
    """Test authentication using a private key string with passphrase."""
    private_key, public_key, passphrase = ssh_keys_with_passphrase

    # Create SFTP resource with private key as string and passphrase
    sftp_resource = SFTPResource(
        host=sftp_server_with_ssh_key_passphrase.get_container_host_ip(),
        port=int(sftp_server_with_ssh_key_passphrase.get_exposed_port(SFTP_PORT)),
        username=SFTP_USERNAME,
        private_key_content=private_key,  # Pass the key content directly
        passphrase=passphrase,
        known_hosts=None,
    )

    # Test basic connection and operations
    # Create a test file
    local_test_file = tmp_path / "test_string_passphrase.txt"
    local_test_file.write_text("Hello from SSH key string with passphrase test!")

    # Upload file
    remote_path = "/upload/ssh_key_string_passphrase_test.txt"
    uploaded = sftp_resource.put_file(local_test_file, remote_path)
    assert uploaded == remote_path

    # Verify file exists
    assert sftp_resource.file_exists(remote_path)

    # Download file
    download_file = tmp_path / "downloaded_string_passphrase.txt"
    downloaded = sftp_resource.get_file(remote_path, download_file)
    assert downloaded == download_file

    # Verify content
    assert (
        download_file.read_text() == "Hello from SSH key string with passphrase test!"
    )

    # Clean up
    sftp_resource.delete_file(remote_path)
    assert not sftp_resource.file_exists(remote_path)


def test_sftp_auth_with_invalid_private_key_string(tmp_path: Path):
    """Test that invalid private key string raises appropriate error."""
    invalid_key = "This is not a valid SSH key"

    # This should fail at connection time
    sftp_resource = SFTPResource(
        host="localhost",
        port=2222,
        username="testuser",
        private_key_content=invalid_key,
        known_hosts=None,
        connect_timeout=5,
    )

    # Should raise an error when trying to connect
    with pytest.raises((asyncssh.Error, OSError, ValueError)):
        sftp_resource.list_files("/")


def test_sftp_auth_with_wrong_passphrase(
    sftp_server_with_ssh_key_passphrase, ssh_keys_with_passphrase, tmp_path: Path
):
    """Test that wrong passphrase for encrypted key fails authentication."""
    private_key, public_key, correct_passphrase = ssh_keys_with_passphrase

    # Write private key to a file
    private_key_file = tmp_path / "test_wrong_pass"
    private_key_file.write_text(private_key)
    private_key_file.chmod(0o600)

    # Create SFTP resource with wrong passphrase
    sftp_resource = SFTPResource(
        host=sftp_server_with_ssh_key_passphrase.get_container_host_ip(),
        port=int(sftp_server_with_ssh_key_passphrase.get_exposed_port(SFTP_PORT)),
        username=SFTP_USERNAME,
        private_key_path=str(private_key_file),
        passphrase="wrong_passphrase",  # Wrong passphrase
        known_hosts=None,
    )

    # Should fail to authenticate
    with pytest.raises(asyncssh.pbe.KeyEncryptionError):
        sftp_resource.list_files("/upload")


def test_sftp_auth_missing_passphrase_for_encrypted_key(
    sftp_server_with_ssh_key_passphrase, ssh_keys_with_passphrase, tmp_path: Path
):
    """Test that missing passphrase for encrypted key fails authentication."""
    private_key, public_key, passphrase = ssh_keys_with_passphrase

    # Create SFTP resource without providing passphrase for encrypted key
    sftp_resource = SFTPResource(
        host=sftp_server_with_ssh_key_passphrase.get_container_host_ip(),
        port=int(sftp_server_with_ssh_key_passphrase.get_exposed_port(SFTP_PORT)),
        username=SFTP_USERNAME,
        private_key_content=private_key,  # Encrypted key
        # No passphrase provided
        known_hosts=None,
    )

    # Should fail to authenticate
    with pytest.raises(asyncssh.KeyImportError):
        sftp_resource.list_files("/upload")

import os
import subprocess
import tempfile
from collections.abc import Generator

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from dagster_sftp.resource import SFTPResource

# SFTP server configuration
SFTP_PORT = 22
SFTP_USERNAME = "testuser"
SFTP_PASSWORD = "testpass"
SFTP_UID = "1000"
SFTP_GID = "1000"


@pytest.fixture(scope="module", autouse=False)
def sftp_server(request) -> Generator[DockerContainer, None, None]:
    """Start an SFTP server container for integration tests."""
    sftp_container = (
        DockerContainer(image="atmoz/sftp:latest")
        .with_exposed_ports(SFTP_PORT)
        .with_command(f"{SFTP_USERNAME}:{SFTP_PASSWORD}:{SFTP_UID}:{SFTP_GID}:upload")
    )
    sftp_container.start()
    wait_for_logs(sftp_container, "Server listening on", timeout=30)

    def cleanup():
        sftp_container.stop()

    request.addfinalizer(cleanup)
    yield sftp_container


@pytest.fixture
def sftp_resource(sftp_server) -> SFTPResource:
    """Create an SFTPResource configured to connect to the test container."""
    return SFTPResource(
        host=sftp_server.get_container_host_ip(),
        port=int(sftp_server.get_exposed_port(SFTP_PORT)),
        username=SFTP_USERNAME,
        password=SFTP_PASSWORD,
        known_hosts=None,
        default_max_requests=128,
        keepalive_interval=15,
    )


def generate_ssh_key_pair() -> tuple[str, str]:
    """Generate an RSA SSH key pair for testing.

    Returns:
        tuple[str, str]: (private_key_content, public_key_content)
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        private_key_path = os.path.join(tmpdir, "test_key")
        public_key_path = f"{private_key_path}.pub"

        # Generate RSA key pair without passphrase
        subprocess.run(
            [
                "ssh-keygen",
                "-t",
                "rsa",
                "-b",
                "2048",
                "-f",
                private_key_path,
                "-N",
                "",  # No passphrase
                "-C",
                "test@example.com",
            ],
            check=True,
            capture_output=True,
            text=True,
        )

        # Read the key contents
        with open(private_key_path) as f:
            private_key = f.read()
        with open(public_key_path) as f:
            public_key = f.read()

        return private_key, public_key


def generate_ssh_key_pair_with_passphrase() -> tuple[str, str, str]:
    """Generate an RSA SSH key pair with passphrase for testing.

    Returns:
        tuple[str, str, str]: (private_key_content, public_key_content, passphrase)
    """
    passphrase = "test_passphrase_123"

    with tempfile.TemporaryDirectory() as tmpdir:
        private_key_path = os.path.join(tmpdir, "test_key_with_pass")
        public_key_path = f"{private_key_path}.pub"

        # Generate RSA key pair with passphrase
        subprocess.run(
            [
                "ssh-keygen",
                "-t",
                "rsa",
                "-b",
                "2048",
                "-f",
                private_key_path,
                "-N",
                passphrase,
                "-C",
                "test@example.com",
            ],
            check=True,
            capture_output=True,
            text=True,
        )

        # Read the key contents
        with open(private_key_path) as f:
            private_key = f.read()
        with open(public_key_path) as f:
            public_key = f.read()

        return private_key, public_key, passphrase


@pytest.fixture(scope="module")
def ssh_keys() -> tuple[str, str]:
    """Generate SSH keys for the test session."""
    return generate_ssh_key_pair()


@pytest.fixture(scope="module")
def ssh_keys_with_passphrase() -> tuple[str, str, str]:
    """Generate SSH keys with passphrase for the test session."""
    return generate_ssh_key_pair_with_passphrase()


@pytest.fixture(scope="module")
def sftp_server_with_ssh_key(ssh_keys) -> Generator[DockerContainer, None, None]:
    """Start an SFTP server container configured for SSH key authentication."""
    private_key, public_key = ssh_keys

    # The atmoz/sftp image expects the public key to be provided in a special format
    # We'll create a temporary directory to hold the SSH key files

    # Create a temporary directory that will persist for the test
    tmpdir = tempfile.mkdtemp()

    try:
        # Write the public key to a file
        pub_key_file = os.path.join(tmpdir, "authorized_keys")
        with open(pub_key_file, "w") as f:
            f.write(public_key)

        # Start container with SSH key authentication
        # The atmoz/sftp expects keys to be mounted in /home/{username}/.ssh/keys/
        sftp_container = (
            DockerContainer(image="atmoz/sftp:latest")
            .with_exposed_ports(SFTP_PORT)
            .with_volume_mapping(
                pub_key_file, f"/home/{SFTP_USERNAME}/.ssh/keys/id_rsa.pub", mode="ro"
            )
            .with_command(
                f"{SFTP_USERNAME}::{SFTP_UID}:{SFTP_GID}:upload"
            )  # Empty password field for SSH key auth
        )

        sftp_container.start()
        wait_for_logs(sftp_container, "Server listening on", timeout=30)

        yield sftp_container

        sftp_container.stop()
    finally:
        # Clean up the temp directory
        import shutil

        shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture(scope="module")
def sftp_server_with_ssh_key_passphrase(
    ssh_keys_with_passphrase,
) -> Generator[DockerContainer, None, None]:
    """Start an SFTP server container configured for SSH key authentication with passphrase."""
    private_key, public_key, passphrase = ssh_keys_with_passphrase

    # Create a temporary directory that will persist for the test
    tmpdir = tempfile.mkdtemp()

    try:
        # Write the public key to a file
        pub_key_file = os.path.join(tmpdir, "authorized_keys")
        with open(pub_key_file, "w") as f:
            f.write(public_key)

        # Start container with SSH key authentication
        sftp_container = (
            DockerContainer(image="atmoz/sftp:latest")
            .with_exposed_ports(SFTP_PORT)
            .with_volume_mapping(
                pub_key_file, f"/home/{SFTP_USERNAME}/.ssh/keys/id_rsa.pub", mode="ro"
            )
            .with_command(
                f"{SFTP_USERNAME}::{SFTP_UID}:{SFTP_GID}:upload"
            )  # Empty password field for SSH key auth
        )

        sftp_container.start()
        wait_for_logs(sftp_container, "Server listening on", timeout=30)

        yield sftp_container

        sftp_container.stop()
    finally:
        # Clean up the temp directory
        import shutil

        shutil.rmtree(tmpdir, ignore_errors=True)

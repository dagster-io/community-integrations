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

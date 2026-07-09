from collections.abc import Iterator
from pathlib import Path
import socket
import subprocess
import time

import boto3
import pytest
from azure.core.exceptions import ResourceExistsError
from azure.storage import blob
from obstore.store import AzureStore


# Make sure unit tests never connect to real AWS
@pytest.fixture(autouse=True)
def fake_aws_credentials(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ENDPOINT", "http://localhost:3000")


@pytest.fixture(autouse=True)
def azurite_credentials(monkeypatch):
    account_name = "devstoreaccount1"
    azure_storage_account_key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
    monkeypatch.setenv("AZURE_STORAGE_USE_EMULATOR", "true")
    monkeypatch.setenv("AZURE_STORAGE_CONTAINER_NAME", "dagster")
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT_KEY", azure_storage_account_key)
    monkeypatch.setenv("AZURITE_BLOB_STORAGE_URL", "http://localhost:10000")
    monkeypatch.setenv("AZURE_STORAGE_ENDPOINT", "http://localhost:10000")
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT_NAME", account_name)
    monkeypatch.setenv(
        "AZURE_STORAGE_CONNECTION_STRING",
        f"DefaultEndpointsProtocol=http;AccountName={account_name};AccountKey={azure_storage_account_key};BlobEndpoint=http://localhost:10000/{account_name};QueueEndpoint=http://localhost:10001/{account_name};",
    )


def _is_port_open(port: int) -> bool:
    try:
        with socket.create_connection(("localhost", port), timeout=0.5):
            return True
    except OSError:
        return False


def _wait_for_ports(ports: tuple[int, ...], timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if all(_is_port_open(port) for port in ports):
            return
        time.sleep(0.5)
    raise RuntimeError(
        f"Timed out waiting for test object storage services on ports {ports}"
    )


@pytest.fixture(scope="session")
def start_object_storage_services() -> Iterator[None]:
    ports = (10000, 3000)
    if all(_is_port_open(port) for port in ports):
        yield
        return

    compose_file = Path(__file__).parents[1] / "docker-compose.yml"
    project_name = "dagster-obstore-tests"
    subprocess.run(
        ["docker", "compose", "-p", project_name, "-f", str(compose_file), "up", "-d"],
        check=True,
    )
    try:
        _wait_for_ports(ports)
        yield
    finally:
        subprocess.run(
            [
                "docker",
                "compose",
                "-p",
                project_name,
                "-f",
                str(compose_file),
                "down",
                "--volumes",
            ],
            check=False,
        )


@pytest.fixture(scope="module")
def start_azurite_server(start_object_storage_services):
    account_name = "devstoreaccount1"
    azure_storage_account_key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

    container_name = "dagster"
    try:
        blob_client = blob.BlobServiceClient.from_connection_string(
            conn_str=f"DefaultEndpointsProtocol=http;AccountName={account_name};AccountKey={azure_storage_account_key};BlobEndpoint=http://localhost:10000/{account_name};QueueEndpoint=http://localhost:10001/{account_name};"
        )
        blob_client.create_container(name=container_name)
    except ResourceExistsError:
        pass

    yield container_name


@pytest.fixture
def container() -> str:
    return "dagster"


@pytest.fixture
def storage_account() -> str:
    return "devstoreaccount1"


@pytest.fixture
def azure_store(container) -> AzureStore:
    return AzureStore(container_name=container, client_options={"allow_http": True})


@pytest.fixture
def mock_s3_resource(start_object_storage_services):
    return boto3.resource(
        "s3",
        region_name="us-east-1",
        endpoint_url="http://localhost:3000",
        verify=False,
        use_ssl=False,
    )


@pytest.fixture
def mock_s3_bucket(mock_s3_resource):
    yield mock_s3_resource.create_bucket(Bucket="test-bucket")

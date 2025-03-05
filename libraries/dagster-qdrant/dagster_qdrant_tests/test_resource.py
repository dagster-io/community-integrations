from uuid import uuid4
from dagster import asset, materialize
import pytest

from dagster_qdrant import QdrantConfig, QdrantResource
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

QDRANT_REST_PORT = 6333
QDRANT_API_KEY = uuid4().hex
DOCUMENTS = [
    "This is a document about oranges",
    "This is a document about pineapples",
    "This is a document about strawberries",
    "This is a document about cucumbers",
]
COLLECTION_NAME = uuid4().hex

qdrant_container = (
    DockerContainer(image="qdrant/qdrant:latest").with_env(
        "QDRANT__SERVICE__API_KEY", QDRANT_API_KEY
    )
).with_exposed_ports(QDRANT_REST_PORT)


@pytest.fixture(scope="module", autouse=True)
def setup_container(request):
    qdrant_container.start()
    wait_for_logs(qdrant_container, "Qdrant HTTP listening")

    def remove_container():
        qdrant_container.stop()

    request.addfinalizer(remove_container)


def test_server_resource():
    @asset
    def create_and_query(qdrant_resource: QdrantResource):
        with qdrant_resource.get_client() as qdrant:
            qdrant.add(
                collection_name=COLLECTION_NAME,
                documents=DOCUMENTS,
            )
            results = qdrant.query(
                collection_name=COLLECTION_NAME, query_text="hawaii", limit=3
            )

            assert len(results) == 3

    result = materialize(
        [create_and_query],
        resources={
            "qdrant_resource": QdrantResource(
                config=QdrantConfig(
                    host=qdrant_container.get_container_host_ip(),
                    port=qdrant_container.get_exposed_port(QDRANT_REST_PORT),
                    api_key=QDRANT_API_KEY,
                    https=False,
                )
            )
        },
    )

    assert result.success


def test_local_resource():
    @asset
    def create_and_query(qdrant_resource: QdrantResource):
        with qdrant_resource.get_client() as qdrant:
            qdrant.add(
                collection_name=COLLECTION_NAME,
                documents=DOCUMENTS,
            )
            results = qdrant.query(
                collection_name=COLLECTION_NAME, query_text="hawaii", limit=3
            )

            assert len(results) == 3

    result = materialize(
        [create_and_query],
        resources={
            "qdrant_resource": QdrantResource(config=QdrantConfig(location=":memory:"))
        },
    )

    assert result.success

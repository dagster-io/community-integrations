import os
import uuid
from collections.abc import Generator

import pytest
from docker.errors import DockerException
from elasticsearch import Elasticsearch
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

ES_IMAGE = os.environ.get("ES_TEST_IMAGE", "docker.elastic.co/elasticsearch/elasticsearch:9.1.3")
ES_PORT = 9200


@pytest.fixture(scope="session")
def es_url() -> Generator[str, None, None]:
    """Yield a base URL for an Elasticsearch cluster.

    Uses the ``ES_URL`` environment variable when set (skips container
    management so a local Docker container or a CI service can be reused).
    Otherwise spins up a single-node container via testcontainers using the
    image in ``ES_TEST_IMAGE`` (default Elasticsearch 9.1.3).

    The ``elasticsearch`` Python client refuses to talk to a server with a
    different major version, so the installed ``elasticsearch`` major must
    match the ``ES_TEST_IMAGE`` major.
    """
    override = os.environ.get("ES_URL")
    if override:
        yield override
        return

    try:
        container = (
            DockerContainer(ES_IMAGE)
            .with_env("discovery.type", "single-node")
            .with_env("xpack.security.enabled", "false")
            .with_env("ES_JAVA_OPTS", "-Xms512m -Xmx512m")
            .with_exposed_ports(ES_PORT)
        )
        container.start()
    except DockerException as exc:
        pytest.skip(f"Docker unavailable for Elasticsearch integration tests: {exc}")
    try:
        wait_for_logs(container, "started")
        host = container.get_container_host_ip()
        port = container.get_exposed_port(ES_PORT)
        yield f"http://{host}:{port}"
    finally:
        container.stop()


@pytest.fixture
def es_client(es_url: str) -> Generator[Elasticsearch, None, None]:
    client = Elasticsearch(hosts=[es_url], verify_certs=False)
    yield client
    client.close()


@pytest.fixture
def index_name(es_client: Elasticsearch) -> Generator[str, None, None]:
    name = f"dagster-test-{uuid.uuid4().hex[:8]}"
    yield name
    # Cleanup any indices matching the prefix (rollover variants).
    matches = es_client.indices.get(
        index=f"{name}*", ignore_unavailable=True, expand_wildcards="all"
    )
    body = matches.body if hasattr(matches, "body") else matches
    for idx in body.keys():
        es_client.indices.delete(index=idx, ignore_unavailable=True)

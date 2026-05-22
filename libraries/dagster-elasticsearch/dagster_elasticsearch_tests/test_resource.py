from dagster import asset, materialize
from elasticsearch import Elasticsearch

from dagster_elasticsearch import ElasticsearchResource, HostsConfig


def test_resource_info(es_url: str) -> None:
    resource = ElasticsearchResource(connection_config=HostsConfig(hosts=[es_url]))
    with resource.get_client() as client:
        info = client.info()
        assert info["version"]["number"].startswith("9.")


def test_resource_round_trip(es_url: str, index_name: str) -> None:
    @asset
    def write_doc(es: ElasticsearchResource) -> None:
        with es.get_client() as client:
            client.index(index=index_name, id="1", document={"title": "hello"})
            client.indices.refresh(index=index_name)

    @asset(deps=[write_doc])
    def read_doc(es: ElasticsearchResource) -> None:
        with es.get_client() as client:
            doc = client.get(index=index_name, id="1")
            assert doc["_source"]["title"] == "hello"

    result = materialize(
        [write_doc, read_doc],
        resources={
            "es": ElasticsearchResource(connection_config=HostsConfig(hosts=[es_url])),
        },
    )
    assert result.success


def test_resource_closes_client(es_url: str) -> None:
    """Client must be closed when the contextmanager exits."""
    resource = ElasticsearchResource(connection_config=HostsConfig(hosts=[es_url]))
    captured: list[Elasticsearch] = []
    with resource.get_client() as client:
        captured.append(client)
    # transport closed → calling info() should fail
    try:
        captured[0].info()
        raise AssertionError("expected closed client to raise")
    except Exception:
        pass

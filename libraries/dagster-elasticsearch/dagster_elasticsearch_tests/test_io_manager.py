from typing import Any

import pytest
from dagster import AssetExecutionContext, StaticPartitionsDefinition, asset, materialize
from elasticsearch import Elasticsearch

from dagster_elasticsearch import (
    ElasticsearchIOManager,
    HostsConfig,
)


def _io(es_url: str, index: str, **kwargs: Any) -> ElasticsearchIOManager:  # noqa: ANN401
    return ElasticsearchIOManager(
        connection_config=HostsConfig(hosts=[es_url]),
        index=index,
        **kwargs,
    )


def test_handle_output_list_dict(es_url: str, index_name: str, es_client: Elasticsearch) -> None:
    @asset
    def docs() -> list[dict]:
        return [
            {"_id": "a", "title": "alpha"},
            {"_id": "b", "title": "beta"},
        ]

    result = materialize([docs], resources={"io_manager": _io(es_url, index_name)})
    assert result.success
    es_client.indices.refresh(index=index_name)
    assert es_client.count(index=index_name)["count"] == 2
    assert es_client.get(index=index_name, id="a")["_source"]["title"] == "alpha"


def test_handle_output_dataframe(es_url: str, index_name: str, es_client: Elasticsearch) -> None:
    pd = pytest.importorskip("pandas")

    @asset
    def docs() -> Any:  # noqa: ANN401
        return pd.DataFrame([{"_id": "1", "n": 10}, {"_id": "2", "n": 20}])

    result = materialize([docs], resources={"io_manager": _io(es_url, index_name)})
    assert result.success
    es_client.indices.refresh(index=index_name)
    assert es_client.count(index=index_name)["count"] == 2


def test_handle_output_polars_lazyframe(
    es_url: str, index_name: str, es_client: Elasticsearch
) -> None:
    """Simulates a parquet-backed Polars LazyFrame from an upstream asset."""
    pl = pytest.importorskip("polars")

    @asset
    def docs() -> Any:  # noqa: ANN401
        return pl.DataFrame([{"_id": str(i), "n": i} for i in range(2500)]).lazy()

    result = materialize(
        [docs], resources={"io_manager": _io(es_url, index_name, bulk_chunk_size=500)}
    )
    assert result.success
    es_client.indices.refresh(index=index_name)
    assert es_client.count(index=index_name)["count"] == 2500


def test_index_config_applies_mappings(
    es_url: str, index_name: str, es_client: Elasticsearch
) -> None:
    from dagster_elasticsearch import ElasticsearchIndexConfig

    @asset
    def docs() -> list[dict]:
        return [{"_id": "1", "title": "hello"}]

    io = _io(
        es_url,
        index_name,
        use_alias=True,
        rollover_strategy="timestamp",
        index_config=ElasticsearchIndexConfig(
            mappings={"properties": {"title": {"type": "keyword"}}},
            settings={"number_of_shards": 1},
        ),
    )
    result = materialize([docs], resources={"io_manager": io})
    assert result.success

    aliased = es_client.indices.get_alias(name=index_name)
    physical = next(iter(aliased.body.keys()))
    mapping = es_client.indices.get_mapping(index=physical).body
    assert mapping[physical]["mappings"]["properties"]["title"]["type"] == "keyword"


def test_load_input_round_trip(es_url: str, index_name: str) -> None:
    @asset
    def producer() -> list[dict]:
        return [{"_id": "1", "v": "x"}, {"_id": "2", "v": "y"}]

    @asset
    def consumer(producer: list[dict]) -> None:
        ids = sorted(d["v"] for d in producer)
        assert ids == ["x", "y"]

    result = materialize(
        [producer, consumer],
        resources={"io_manager": _io(es_url, index_name)},
    )
    assert result.success


def test_alias_swap(es_url: str, index_name: str, es_client: Elasticsearch) -> None:
    payload = {"v": "first"}

    @asset
    def docs() -> list[dict]:
        return [{"_id": "1", **payload}]

    io = _io(es_url, index_name, use_alias=True, rollover_strategy="timestamp")
    result = materialize([docs], resources={"io_manager": io})
    assert result.success

    # alias points at one physical index
    aliased = es_client.indices.get_alias(name=index_name)
    physical_indices = list(aliased.body.keys() if hasattr(aliased, "body") else aliased.keys())
    assert len(physical_indices) == 1
    first_index = physical_indices[0]
    assert first_index.startswith(f"{index_name}-")

    payload["v"] = "second"

    # second materialization → new physical index, alias atomically swapped
    result = materialize([docs], resources={"io_manager": io})
    assert result.success

    aliased = es_client.indices.get_alias(name=index_name)
    physical_indices = list(aliased.body.keys() if hasattr(aliased, "body") else aliased.keys())
    assert len(physical_indices) == 1
    second_index = physical_indices[0]
    assert second_index != first_index
    # Reading via alias returns latest data
    es_client.indices.refresh(index=index_name)
    doc = es_client.get(index=index_name, id="1")
    assert doc["_source"]["v"] == "second"


def test_alias_keep_last_cleanup(es_url: str, index_name: str, es_client: Elasticsearch) -> None:
    @asset
    def docs() -> list[dict]:
        return [{"_id": "1", "v": "x"}]

    io = _io(
        es_url,
        index_name,
        use_alias=True,
        rollover_strategy="timestamp",
        keep_last=2,
    )
    for _ in range(4):
        materialize([docs], resources={"io_manager": io})

    matching = es_client.indices.get(index=f"{index_name}-*")
    names = list(matching.body.keys() if hasattr(matching, "body") else matching.keys())
    assert len(names) == 2, f"expected keep_last=2, got {names}"


def test_partitioned_asset_index_per_partition(
    es_url: str, index_name: str, es_client: Elasticsearch
) -> None:
    parts = StaticPartitionsDefinition(["p1", "p2"])

    @asset(partitions_def=parts)
    def docs(context: AssetExecutionContext) -> list[dict]:
        return [{"_id": context.partition_key, "p": context.partition_key}]

    io = _io(es_url, index_name)
    for key in ("p1", "p2"):
        result = materialize([docs], resources={"io_manager": io}, partition_key=key)
        assert result.success

    for key in ("p1", "p2"):
        physical = f"{index_name}-{key}"
        es_client.indices.refresh(index=physical)
        doc = es_client.get(index=physical, id=key)
        assert doc["_source"]["p"] == key


def test_alias_rollover_partition_strategy(
    es_url: str, index_name: str, es_client: Elasticsearch
) -> None:
    parts = StaticPartitionsDefinition(["a", "b"])

    @asset(partitions_def=parts)
    def docs(context: AssetExecutionContext) -> list[dict]:
        return [{"_id": "1", "k": context.partition_key}]

    io = _io(es_url, index_name, use_alias=True, rollover_strategy="auto")
    materialize([docs], resources={"io_manager": io}, partition_key="a")
    materialize([docs], resources={"io_manager": io}, partition_key="b")

    aliased = es_client.indices.get_alias(name=index_name)
    physical = list(aliased.body.keys() if hasattr(aliased, "body") else aliased.keys())
    # Latest write (partition b) holds the alias
    assert physical == [f"{index_name}-b"]


def test_handle_output_empty_list(es_url: str, index_name: str, es_client: Elasticsearch) -> None:
    """Empty input must not error and should not create stray indices."""

    @asset
    def docs() -> list[dict]:
        return []

    result = materialize([docs], resources={"io_manager": _io(es_url, index_name)})
    assert result.success
    # No documents indexed, but no failure either.
    assert not es_client.indices.exists(index=index_name)


def test_handle_output_empty_with_alias_creates_index(
    es_url: str, index_name: str, es_client: Elasticsearch
) -> None:
    """Empty input + alias rollover still creates the rollover index and alias."""

    @asset
    def docs() -> list[dict]:
        return []

    io = _io(es_url, index_name, use_alias=True, rollover_strategy="timestamp")
    result = materialize([docs], resources={"io_manager": io})
    assert result.success
    assert es_client.indices.exists_alias(name=index_name)


def test_special_chars_in_id(es_url: str, index_name: str, es_client: Elasticsearch) -> None:
    """Document _id values containing slashes/colons must round-trip."""

    @asset
    def docs() -> list[dict]:
        return [
            {"_id": "ns:resource/123", "title": "first"},
            {"_id": "https://example.com/x", "title": "second"},
        ]

    result = materialize([docs], resources={"io_manager": _io(es_url, index_name)})
    assert result.success
    es_client.indices.refresh(index=index_name)
    # Use search to dodge URL-encoding pitfalls in client.get().
    hits = es_client.search(index=index_name, query={"match_all": {}}).body["hits"]["hits"]
    ids = sorted(h["_id"] for h in hits)
    assert ids == ["https://example.com/x", "ns:resource/123"]


def test_refresh_disabled(es_url: str, index_name: str, es_client: Elasticsearch) -> None:
    """When refresh=False the IO manager skips the post-write refresh."""

    @asset
    def docs() -> list[dict]:
        return [{"_id": "1", "v": "x"}]

    result = materialize([docs], resources={"io_manager": _io(es_url, index_name, refresh=False)})
    assert result.success
    # An explicit refresh after the fact should still find the doc.
    es_client.indices.refresh(index=index_name)
    assert es_client.count(index=index_name)["count"] == 1


def test_load_input_missing_index_returns_empty(
    es_url: str, index_name: str, es_client: Elasticsearch
) -> None:
    """Reading from a non-existent index yields an empty list, not an error."""
    from dagster import build_input_context

    from dagster_elasticsearch import ElasticsearchIOManager, HostsConfig

    io = ElasticsearchIOManager(
        connection_config=HostsConfig(hosts=[es_url]),
        index=index_name,
    )
    ctx = build_input_context()
    docs = io.load_input(ctx)
    assert docs == []

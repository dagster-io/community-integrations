# dagster-elasticsearch

A Dagster module that integrates with [Elasticsearch 9.x](https://www.elastic.co/elasticsearch). It exposes:

- `ElasticsearchResource` — a thin `ConfigurableResource` over the official `elasticsearch` Python client.
- `ElasticsearchIOManager` — a `ConfigurableIOManager` that bulk-indexes asset outputs as documents, with optional **alias rollover** for zero-downtime atomic swaps.

> Elasticsearch 9.x is required. Per the elasticsearch-py compatibility policy, the 9.x client refuses to talk to an 8.x server.

## Installation

```sh
uv pip install dagster-elasticsearch
# Optional: pandas DataFrame support in the IO manager.
uv pip install 'dagster-elasticsearch[pandas]'
```

## Resource

```python
from dagster import Definitions, EnvVar, asset
from dagster_elasticsearch import ElasticsearchResource, HostsConfig

@asset
def index_docs(es: ElasticsearchResource) -> None:
    with es.get_client() as client:
        client.index(index="docs", document={"title": "hello"})
        client.indices.refresh(index="docs")

defs = Definitions(
    assets=[index_docs],
    resources={
        "es": ElasticsearchResource(
            connection_config=HostsConfig(
                hosts=["https://es.example.com:9200"],
                api_key=EnvVar("ES_API_KEY"),
            ),
        ),
    },
)
```

### Authentication

`HostsConfig` and `CloudConfig` both accept either an `api_key` or a `username` + `password` pair (basic auth). `HostsConfig` also accepts `bearer_auth`, `ca_certs`, and `verify_certs`.

```python
from dagster_elasticsearch import CloudConfig, HostsConfig

# Elastic Cloud
CloudConfig(cloud_id="my-deployment:...", api_key=EnvVar("ES_API_KEY"))

# Self-hosted, basic auth
HostsConfig(
    hosts=["https://es.internal:9200"],
    username="elastic",
    password=EnvVar("ES_PASSWORD"),
    ca_certs="/etc/ssl/certs/elastic.crt",
)
```

## IO manager

`ElasticsearchIOManager` accepts `dict`, `list[dict]`, or `pandas.DataFrame` outputs and bulk-indexes them via `elasticsearch.helpers.bulk`. The `id_field` (default `_id`) is lifted from each document to become the Elasticsearch `_id`.

```python
from dagster import Definitions, asset
from dagster_elasticsearch import ElasticsearchIOManager, HostsConfig

@asset
def docs() -> list[dict]:
    return [{"_id": "1", "title": "hello"}]

defs = Definitions(
    assets=[docs],
    resources={
        "io_manager": ElasticsearchIOManager(
            connection_config=HostsConfig(hosts=["http://localhost:9200"]),
            index="docs",
        ),
    },
)
```

### Alias rollover

Set `use_alias=True` and the IO manager writes every materialisation to a fresh physical index, then atomically swaps a stable alias to the new index via the `_aliases` API. Readers and downstream assets always see a consistent view via the alias name.

```python
ElasticsearchIOManager(
    connection_config=HostsConfig(hosts=["http://localhost:9200"]),
    index="docs",          # alias name; physical indices are docs-<suffix>
    use_alias=True,
    rollover_strategy="auto",  # partition key for partitioned assets, else timestamp
    keep_last=3,           # delete older rollover indices beyond N
)
```

Rollover strategies:

| Strategy     | Suffix                                     | Notes                                       |
|--------------|--------------------------------------------|---------------------------------------------|
| `auto`       | `partition` if partitioned else `timestamp`| Default.                                    |
| `timestamp`  | `YYYYMMDDtHHMMSSzMICROS`                   | Microsecond precision avoids collisions.    |
| `run_id`     | Dagster run id                             | Opaque but unique per run.                  |
| `partition`  | Slugified partition key                    | Errors if asset isn't partitioned.          |
| `none`       | (empty)                                    | Alias swap is a no-op; mostly for testing.  |

### Partitioned assets (without alias)

When `use_alias=False` and the asset is partitioned, each partition writes to its own index `{index}-{partition_key}`. Reads target the same per-partition index.

## Development

```sh
make test    # requires Docker for the Elasticsearch testcontainer
make ruff
make check
```

Set `ES_URL` to reuse an already-running cluster instead of spinning up a fresh container per test session:

```sh
ES_URL=http://localhost:9200 uv run pytest
```

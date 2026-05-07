# dagster-elasticsearch

A Dagster integration for [Elasticsearch](https://www.elastic.co/elasticsearch). Two pieces:

- `ElasticsearchResource` is a thin `ConfigurableResource` over the official `elasticsearch` Python client.
- `ElasticsearchIOManager` bulk-indexes asset outputs as documents, with optional **alias rollover** for atomic, zero-downtime swaps.

## Installation

```sh
uv pip install dagster-elasticsearch

# Optional DataFrame / table format support in the IO manager.
uv pip install 'dagster-elasticsearch[pandas]'
uv pip install 'dagster-elasticsearch[polars]'
uv pip install 'dagster-elasticsearch[arrow]'
```

### Picking an Elasticsearch version

`elasticsearch-py` won't talk to a server with a different major version, so this package is loose-pinned (`elasticsearch>=8.10,<11`) and leaves the major up to your project. Pin it yourself in `pyproject.toml`:

```toml
# 8.x
dependencies = ["dagster-elasticsearch", "elasticsearch>=8.10,<9"]

# 9.x
dependencies = ["dagster-elasticsearch", "elasticsearch>=9,<10"]

# 10.x (when available)
dependencies = ["dagster-elasticsearch", "elasticsearch>=10,<11"]
```

The bulk and alias APIs the integration uses haven't changed across 8, 9, and 10.

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

`HostsConfig` and `CloudConfig` both accept either an `api_key` or a `username` + `password` pair. `HostsConfig` also takes `bearer_auth`, `ca_certs`, and `verify_certs`.

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

`ElasticsearchIOManager` takes `dict`, `list[dict]`, `pandas.DataFrame`, or Polars `DataFrame`/`LazyFrame` outputs and bulk-indexes them via `elasticsearch.helpers.bulk`. The `id_field` (default `_id`) is lifted from each document and used as the Elasticsearch `_id`.

DataFrame inputs are streamed in `bulk_chunk_size`-row slices. Polars `LazyFrame`s are collected with the streaming engine. Neither is materialised in full before indexing, so very large parquet-backed assets stay memory-bounded.

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

With `use_alias=True`, every materialisation writes to a fresh physical index, then atomically swaps a stable alias to the new index via `_aliases`. Readers and downstream assets always see a consistent view via the alias name.

```python
ElasticsearchIOManager(
    connection_config=HostsConfig(hosts=["http://localhost:9200"]),
    index="docs",          # alias name; physical indices are docs-<suffix>
    use_alias=True,
    rollover_strategy="auto",  # partition key for partitioned assets, else timestamp
    keep_last=3,           # delete older rollover indices beyond N
)
```

| Strategy     | Suffix                                     | Notes                                    |
|--------------|--------------------------------------------|------------------------------------------|
| `auto`       | `partition` if partitioned, else `timestamp` | Default.                                 |
| `timestamp`  | `YYYYMMDDtHHMMSSzMICROS`                   | Microsecond precision avoids collisions. |
| `run_id`     | Dagster run id                             | Opaque but unique per run.               |
| `partition`  | Slugified partition key                    | Errors if the asset isn't partitioned.   |
| `none`       | (empty)                                    | Alias swap is a no-op. Mostly for tests. |

### Partitioned assets (without alias)

When `use_alias=False` and the asset is partitioned, each partition writes to its own index `{index}-{partition_key}`. Reads target the same per-partition index.

> **Note on multi-partition loads.** `ElasticsearchIOManager` is a plain `IOManager`, not a `UPathIOManager`. Loading inputs that span multiple partitions (e.g. a downstream asset depending on every partition of an upstream) calls `load_input` once per partition. Fine for tens of partitions; for hundreds-or-thousands you may want a different read strategy (one ES query against the alias, for instance, or a dedicated downstream asset that reads via the resource and not the IO manager).

### Per-asset overrides

Most IO manager fields can be overridden per-asset via `definition_metadata` (or `output_metadata` for per-output overrides). Resolution order: `output_metadata` > `definition_metadata` > resource-level config.

```python
@asset(metadata={
    "index": "search-docs",
    "bulk_chunk_size": 100,
    "rollover_strategy": "run_id",
})
def docs() -> list[dict]:
    ...
```

Supported keys: `index`, `id_field`, `bulk_chunk_size`, `max_chunk_bytes`, `refresh`, `rollover_strategy`, `index_config`.

### Lazy reads

For very large source indices, set `lazy_load=True` so `load_input` returns an iterator that streams hits via the scroll API instead of building a `list[dict]`:

```python
ElasticsearchIOManager(
    connection_config=HostsConfig(hosts=["http://localhost:9200"]),
    index="docs",
    lazy_load=True,
    scan_size=1000,           # page size
    scroll_keep_alive="5m",   # scroll context lifetime
)
```

### Output metadata

Each materialisation records the following on the asset:

| Key        | Type | When                                           |
|------------|------|------------------------------------------------|
| `index`    | text | Always. Physical index that was written to.    |
| `indexed`  | int  | Always. Number of documents indexed.           |
| `failures` | int  | When `fail_fast=False` and some docs failed.   |
| `alias`    | text | When `use_alias=True`. The stable alias name.  |

### Index mappings and settings

Pass `index_config` to control mappings and shard/replica settings on indices the IO manager creates (alias rollover only):

```python
from dagster_elasticsearch import ElasticsearchIndexConfig, ElasticsearchIOManager, HostsConfig

ElasticsearchIOManager(
    connection_config=HostsConfig(hosts=["http://localhost:9200"]),
    index="docs",
    use_alias=True,
    index_config=ElasticsearchIndexConfig(
        mappings={
            "properties": {
                "title": {"type": "text"},
                "tags": {"type": "keyword"},
            }
        },
        settings={"number_of_shards": 3, "number_of_replicas": 1},
    ),
)
```

### Polars / Parquet handoff

If an upstream asset uses `dagster-polars`'s `PolarsParquetIOManager` and a downstream asset writes to Elasticsearch, the downstream can take a `LazyFrame` straight from the upstream parquet:

```python
import dagster as dg
import polars as pl

@dg.asset(io_manager_key="parquet_io_manager")
def raw_records() -> pl.DataFrame:
    return pl.DataFrame([...])

@dg.asset(io_manager_key="es_io_manager")
def search_records(raw_records: pl.LazyFrame) -> pl.LazyFrame:
    return raw_records.filter(pl.col("active"))
```

The IO manager collects the `LazyFrame` with Polars's streaming engine and yields `bulk_chunk_size`-row slices into the Elasticsearch bulk helper, so the full result never has to fit in RAM.

### Error handling

Per-document indexing errors raise `ElasticsearchBulkIndexError`. The underlying error list is on `.errors`:

```python
from dagster_elasticsearch import ElasticsearchBulkIndexError

try:
    materialize([docs], resources={...})
except ElasticsearchBulkIndexError as e:
    for err in e.errors:
        print(err)
```

Set `fail_fast=False` on the IO manager to log per-document errors instead of aborting the materialisation.

## Troubleshooting

**Cluster status `red` and shards stuck unassigned in local Docker.** Elasticsearch refuses to allocate shards once the host disk is over the high-watermark threshold (default 90%). Either free up space, or (for local development only) disable the threshold:

```sh
curl -XPUT http://localhost:9200/_cluster/settings \
  -H 'Content-Type: application/json' \
  -d '{"persistent":{"cluster.routing.allocation.disk.threshold_enabled":"false"}}'
```

**`elasticsearch.BadRequestError: invalid_index_name_exception ... must be lowercase`.** Index names have to be lowercase. The IO manager lowercases timestamp suffixes itself, but if you supply your own index name make sure it is lowercase too.

## Development

```sh
make test    # requires Docker for the Elasticsearch testcontainer
make ruff
make check
```

Set `ES_URL` to reuse a running cluster instead of spinning up a fresh container per test session:

```sh
ES_URL=http://localhost:9200 uv run pytest
```

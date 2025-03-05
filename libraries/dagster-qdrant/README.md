# dagster-qdrant

A Dagster module that provides an integration with [Qdrant](https://qdrant.tech/).

## Installation

The `dagster_qdrant` module is available as a PyPI package - install with your preferred python
environment manager (We recommend [uv](https://github.com/astral-sh/uv)).

```
source .venv/bin/activate
uv pip install dagster-qdrant
```

## Example Usage

You can get a free-forever cloud instance from [cloud.qdrant.io](http://cloud.qdrant.io).

```python
from dagster_qdrant import QdrantConfig, QdrantResource

import dagster as dg


@dg.asset
def my_table(qdrant_resource: QdrantResource):
    with qdrant_resource.get_client() as qdrant:
            qdrant.add(
                collection_name="test_collection",
                documents=[
                    "This is a document about oranges",
                    "This is a document about pineapples",
                    "This is a document about strawberries",
                    "This is a document about cucumbers",
                ],
            )
            results = qdrant.query(
                collection_name="test_collection", query_text="hawaii", limit=3
            )


defs = dg.Definitions(
    assets=[my_table],
    resources={
        "qdrant_resource": QdrantResource(
            config=QdrantConfig(
                host="xyz-example.eu-central.aws.cloud.qdrant.io",
                api_key="<your-api-key>",
            )
        )
    },
)

```

## Development

The `Makefile` provides the tools required to test and lint your local installation.

```sh
make test
make ruff
make check
```

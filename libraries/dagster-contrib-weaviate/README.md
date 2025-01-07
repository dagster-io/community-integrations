# dagster-contrib-weaviate

A dagster module that provides integration with [Weaviate](https://weaviate.io/)
(both Cloud and Self-Hosted Weaviate instances).

This module provides the `WeaviateLocalResource` and the `WeaviateCloudResource` for integration 
with a self-hosted or WeaviateCloud instance, respectively.

## Installation

The `dagster_contrib_weaviate` module is available as a PyPI package - install with your preferred python 
environment manager (We recommend [uv](https://github.com/astral-sh/uv)).

```
source .venv/bin/activate
uv pip install dagster_contrib_weaviate
```

## Example Usage (Local Weaviate Instance)

(Based on the [Weaviate Quickstart Guide (Local)](https://weaviate.io/developers/weaviate/quickstart/local))

```python
from dagster import Definitions, asset
from dagster_contrib_weaviate import WeaviateLocalResource

@asset
def my_table(weaviate: WeaviateLocalResource):
    with weaviate.get_client() as weaviate_client:
        questions = weaviate_client.collections.get("Question")
        questions.query.near_text(query="biology", limit=2)

defs = Definitions(
    assets=[my_table],
    resources={"weaviate": WeaviateLocalResource(host="192.168.0.10")}
)
```


## Example Usage (Weaviate Cloud Instance)

Based on the [Weaviate Cloud Quickstart Guide](https://weaviate.io/developers/wcs/quickstart)

```python
from dagster import Definitions, asset
from dagster_contrib_weaviate import WeaviateCloudResource

@asset
def my_table(weaviate: WeaviateCloudResource):
    with weaviate.get_client() as weaviate_client:
        questions = weaviate_client.collections.get("Question")
        questions.query.near_text(query="biology", limit=2)

defs = Definitions(
    assets=[my_table],
    resources={
        "weaviate": WeaviateCloudResource(
            cluster_url=wcd_url,
            auth_credentials={
                "api_key": wcd_apikey
            },
            headers={
                "X-Cohere-Api-Key": cohere_apikey,
            }
        ),
    },
)
```



## Development

The `Makefile` provides the tools required to test and lint your local installation

```sh
make test
make ruff
make check
```
# dagster-contrib-weaviate

A dagster module that provides integration with [Weaviate](https://weaviate.io/)
(both Cloud and Self-Hosted Weaviate instances).

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
from dagster_contrib_weaviate import WeaviateResource, LocalConfig

@asset
def my_table(weaviate: WeaviateResource):
    with weaviate.get_client() as weaviate_client:
        questions = weaviate_client.collections.get("Question")
        questions.query.near_text(query="biology", limit=2)

defs = Definitions(
    assets=[my_table],
    resources={
        "weaviate": WeaviateResource(
            connection_config=LocalConfig(
                host="192.168.0.10", 
                port=8080, 
            )
        ),
    },
)
```


## Example Usage (Weaviate Cloud Instance)

Based on the [Weaviate Cloud Quickstart Guide](https://weaviate.io/developers/wcs/quickstart)

```python
from dagster import Definitions, asset
from dagster_contrib_weaviate import WeaviateResource, CloudConfig

@asset
def my_table(weaviate: WeaviateResource):
    with weaviate.get_client() as weaviate_client:
        questions = weaviate_client.collections.get("Question")
        questions.query.near_text(query="biology", limit=2)

defs = Definitions(
    assets=[my_table],
    resources={
        "weaviate": WeaviateResource(
            connection_config=CloudConfig(
                cluster_url=wcd_url
            ),
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
# dagster-chroma

A dagster module that provides integration with [chroma](https://www.trychroma.com/).
Both HTTP connections, and local (filesystem-based) connections are supported.

## Installation

The `dagster_chroma` module is available as a PyPI package - install with your preferred python
environment manager (We recommend [uv](https://github.com/astral-sh/uv)).

```
source .venv/bin/activate
uv pip install dagster-chroma
```

## Example Usage (Local Chroma Instance)

(Based on the [Chroma getting started guide](https://docs.trychroma.com/docs/overview/getting-started))

```python
import os
from dagster import Definitions, asset
from dagster_chroma import ChromaResource, LocalConfig, HttpConfig

@asset
def my_table(chroma: ChromaResource):
    with chroma.get_client() as chroma_client:
        collection = chroma_client.create_collection("fruits")

        collection.add(
            documents=[
                "This is a document about oranges",
                "This is a document about pineapples",
                "This is a document about strawberries",
                "This is a document about cucumbers"],
            ids=["oranges", "pineapples", "strawberries", "cucumbers"],
        )

        results = collection.query(
            query_texts=["hawaii"],
            n_results=1,
        )

defs = Definitions(
    assets=[my_table],
    resources={
        "chroma": ChromaResource(
            connection_config=
                LocalConfig(persistence_path="./chroma") if os.getenv("DEV") else
                    HttpConfig(host="192.168.0.10", port=8000)
        ),
    }
)
```

## Development

The `Makefile` provides the tools required to test and lint your local installation.

```sh
make test
make ruff
make check
```

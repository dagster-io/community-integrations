# dagster-vertexai

A dagster module that provides integration with [vertexai](https://cloud.google.com/vertex-ai/generative-ai/docs/start/quickstart).

## Installation

The `dagster_vertexai` module is available as a PyPI package - install with your preferred python
environment manager (We recommend [uv](https://github.com/astral-sh/uv)).

```
source .venv/bin/activate
uv pip install dagster-vertexai
```

## Example Usage

In addition to wrapping the vertexai GenerativeModel class (get_model/get_model_for_asset methods),
this resource logs the usage of the vertexai API to to the asset metadata (both number of calls, and tokens).
This is achieved by wrapping the GenerativeModel.generate_content method.

Note that the usage will only be logged to the asset metadata from an Asset context -
not from an Op context.
Also note that only the synchronous API usage metadata will be automatically logged -
not the streaming or batching API.

```python
from dagster import AssetExecutionContext, Definitions, EnvVar, asset, define_asset_job
from dagster_vertexai import vertexaiResource


@asset(compute_kind="vertexai")
def vertexai_asset(context: AssetExecutionContext, vertexai: vertexaiResource):
    with vertexai.get_model(context) as model:
        response = model.generate_content(
            "Generate a short sentence on tests"
        )

defs = Definitions(
    assets=[vertexai_asset],
    resources={
        "vertexai": vertexaiResource(
            api_key=EnvVar("vertexai_API_KEY"),
            generative_model_name="vertexai-1.5-flash"
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

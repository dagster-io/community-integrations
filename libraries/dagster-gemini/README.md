# dagster-gemini

A dagster module that provides integration with [Gemini](https://deepmind.google/technologies/gemini/).

## Installation

The `dagster_gemini` module is available as a PyPI package - install with your preferred python
environment manager (We recommend [uv](https://github.com/astral-sh/uv)).

```
source .venv/bin/activate
uv pip install dagster-gemini
```

## Example Usage

In addition to wrapping the Gemini GenerativeModel class (get_model/get_model_for_asset methods),
this resource logs the usage of the Gemini API to to the asset metadata (both number of calls, and tokens).
This is achieved by wrapping the GenerativeModel.generate_content method.

Note that the usage will only be logged to the asset metadata from an Asset context -
not from an Op context.
Also note that only the synchronous API usage metadata will be automatically logged -
not the streaming or batching API.

```python
from dagster import AssetExecutionContext, Definitions, EnvVar, asset, define_asset_job
from dagster_gemini import GeminiResource


@asset(compute_kind="gemini")
def gemini_asset(context: AssetExecutionContext, gemini: GeminiResource):
    with gemini.get_model(context) as model:
        response = model.generate_content(
            "Generate a short sentence on tests"
        )

defs = Definitions(
    assets=[gemini_asset],
    resources={
        "gemini": GeminiResource(
            api_key=EnvVar("GEMINI_API_KEY"),
            generative_model_name="gemini-1.5-flash"
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

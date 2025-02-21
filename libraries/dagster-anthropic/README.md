# dagster-anthropic

A dagster module that provides integration with [Anthropic](https://www.anthropic.com//).

## Installation

The `dagster_anthropic` module is available as a PyPI package - install with your preferred python
environment manager (We recommend [uv](https://github.com/astral-sh/uv)).

```
source .venv/bin/activate
uv pip install dagster-anthropic
```

## Example Usage

In addition to wrapping the Anthropic client (get_client/get_client_for_asset methods),
this resource logs the usage of the Anthropic API to to the asset metadata (both number of calls, and tokens).
This is achieved by wrapping the Anthropic.messages.create method.

Note that the usage will only be logged to the asset metadata from an Asset context -
not from an Op context.
Also note that only the synchronous API usage metadata will be automatically logged -
not the streaming or batching API.

```python
from dagster import AssetExecutionContext, Definitions, EnvVar, asset, define_asset_job
from dagster_anthropic import AnthropicResource


@asset(compute_kind="anthropic")
def anthropic_asset(context: AssetExecutionContext, anthropic: AnthropicResource):
    with anthropic.get_client(context) as client:
        response = client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=1024,
            messages=[
                {"role": "user", "content": "Say this is a test"}
            ]
        )

defs = Definitions(
    assets=[anthropic_asset],
    resources={
        "anthropic": AnthropicResource(api_key=EnvVar("ANTHROPIC_API_KEY")),
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

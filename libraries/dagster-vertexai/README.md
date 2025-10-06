# dagster-vertexai

A dagster module that provides integration with [Google Cloud Vertex AI](https://cloud.google.com/vertex-ai/generative-ai/docs/start/quickstart).

## Installation

The `dagster_vertexai` module is available as a PyPI package - install with your preferred python
environment manager (We recommend [uv](https://github.com/astral-sh/uv)).

```
source .venv/bin/activate
uv pip install dagster-vertexai
```

## Example Usage

In addition to wrapping the Vertex AI GenerativeModel class (get_model/get_model_for_asset methods),
this resource logs the usage of the Vertex AI API to the asset metadata (both number of calls, and tokens).
This is achieved by wrapping the GenerativeModel.generate_content method.

Note that the usage will only be logged to the asset metadata from an Asset context -
not from an Op context.
Also note that only the synchronous API usage metadata will be automatically logged -
not the streaming or batching API.

```python
from dagster import AssetExecutionContext, Definitions, EnvVar, asset
from dagster_vertexai import VertexAIResource


@asset(compute_kind="vertexai")
def vertexai_asset(context: AssetExecutionContext, vertexai: VertexAIResource):
    with vertexai.get_model(context) as model:
        response = model.generate_content(
            "Generate a short sentence on tests"
        )
        return response.text

defs = Definitions(
    assets=[vertexai_asset],
    resources={
        "vertexai": VertexAIResource(
            project_id=EnvVar("VERTEX_AI_PROJECT_ID"),
            location=EnvVar("VERTEX_AI_LOCATION"),
            generative_model_name="gemini-1.5-flash-001"
        ),
    },
)
```

## Configuration

The `VertexAIResource` supports the following configuration options:

- `project_id`: Your Google Cloud project ID
- `location`: The Google Cloud region (e.g., "us-central1")
- `generative_model_name`: The name of the generative model (e.g., "gemini-1.5-flash-001")
- `google_credentials` (optional): JSON string of service account credentials. If not provided, uses Application Default Credentials
- `api_endpoint` (optional): Custom API endpoint URL

## Authentication

Authentication is handled via [Application Default Credentials (ADC)](https://cloud.google.com/docs/authentication/application-default-credentials) by default. To authenticate:

```bash
gcloud auth application-default login
```

Alternatively, you can provide service account credentials via the `google_credentials` parameter.

## Multi-Asset Usage

For multi-assets, use the `get_model_for_asset` method to specify which asset the usage metadata should be logged to:

```python
from dagster import AssetExecutionContext, AssetKey, AssetSpec, MaterializeResult, multi_asset

@multi_asset(
    specs=[AssetSpec("summary"), AssetSpec("analysis")]
)
def vertexai_multi_asset(context: AssetExecutionContext, vertexai: VertexAIResource):
    # Generate summary
    with vertexai.get_model_for_asset(context, AssetKey("summary")) as model:
        summary = model.generate_content("Summarize this data...")
    
    # Generate analysis  
    with vertexai.get_model_for_asset(context, AssetKey("analysis")) as model:
        analysis = model.generate_content("Analyze this data...")
    
    yield MaterializeResult(asset_key="summary", metadata={"content": summary.text})
    yield MaterializeResult(asset_key="analysis", metadata={"content": analysis.text})
```

## Development

The `Makefile` provides the tools required to test and lint your local installation

```sh
make test
make ruff
make check
```
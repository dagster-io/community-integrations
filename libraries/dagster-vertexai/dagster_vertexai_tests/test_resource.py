import pytest
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetSpec,
    MaterializeResult,
    OpExecutionContext,
    asset,
    job,
    materialize,
    multi_asset,
    op,
)
from dagster._core.errors import DagsterInvariantViolationError

from dagster_vertexai import VertexAIResource

# --- Integration Test Configuration ---
# These must be valid values for your GCP environment.
# Ensure you are authenticated via `gcloud auth application-default login`.
PROJECT_ID = "my-project"
LOCATION = "us-central1"
# Use a valid and available model name
MODEL_NAME = "gemini-2.5-flash-lite"

# This is the resource that all integration tests will use.
VERTEX_AI_RESOURCE_CONFIG = {
    "vertexai": VertexAIResource(
        project_id=PROJECT_ID, location=LOCATION, generative_model_name=MODEL_NAME
    )
}


# --- Tests ---


def test_resource_initialization():
    """Tests that the resource can be initialized without errors."""
    try:
        VertexAIResource(
            project_id=PROJECT_ID, location=LOCATION, generative_model_name=MODEL_NAME
        )
    except Exception as e:
        pytest.fail(f"VertexAIResource initialization failed: {e}")


def test_vertexai_resource_with_op():
    """Tests that the resource can be used within an op."""

    @op
    def vertexai_op(context: OpExecutionContext, vertexai: VertexAIResource):
        with vertexai.get_model(context=context) as model:
            response = model.count_tokens("hello")
            assert response.total_tokens > 0

    @job
    def vertexai_op_job():
        vertexai_op()

    result = vertexai_op_job.execute_in_process(resources=VERTEX_AI_RESOURCE_CONFIG)
    assert result.success


def test_vertexai_resource_with_asset():
    """Tests that the resource can be used within an asset."""

    @asset
    def vertexai_asset(context: AssetExecutionContext, vertexai: VertexAIResource):
        with vertexai.get_model(context=context) as model:
            response = model.generate_content("hello there")
            assert response.text is not None

    result = materialize([vertexai_asset], resources=VERTEX_AI_RESOURCE_CONFIG)
    assert result.success


def test_vertexai_resource_with_graph_backed_asset():
    """Tests resource usage in a simple asset that calls an op."""

    @asset
    def vertexai_graph_asset(
        context: AssetExecutionContext, vertexai: VertexAIResource
    ) -> str:
        # Simple test that combines multiple operations
        message = "Generate a two-word greeting."

        with vertexai.get_model(context=context) as model:
            response = model.generate_content(message)
            return response.text or "No response"

    result = materialize([vertexai_graph_asset], resources=VERTEX_AI_RESOURCE_CONFIG)
    assert result.success


def test_vertexai_resource_with_multi_asset():
    """Tests multi-asset scenarios with the live resource."""

    # Fixed: Yielding MaterializeResult is the modern, type-safe pattern.
    @multi_asset(
        specs=[AssetSpec("status"), AssetSpec("result")],
    )
    def vertexai_multi_asset(
        context: AssetExecutionContext, vertexai: VertexAIResource
    ):
        with vertexai.get_model_for_asset(
            context=context, asset_key=AssetKey("result")
        ) as model:
            response = model.generate_content("Say hello")
            assert response.text

        with pytest.raises(DagsterInvariantViolationError):
            with vertexai.get_model(context=context):
                pass

        yield MaterializeResult(asset_key="status", metadata={"value": "ok"})
        yield MaterializeResult(asset_key="result", metadata={"value": "done"})

    result = materialize([vertexai_multi_asset], resources=VERTEX_AI_RESOURCE_CONFIG)
    assert result.success


def test_vertexai_wrapper_with_asset():
    """Tests that the metadata wrapper correctly logs usage for a simple asset."""

    @asset
    def vertexai_asset(context: AssetExecutionContext, vertexai: VertexAIResource):
        with vertexai.get_model(context=context) as model:
            model.generate_content("say this is a test")
        return MaterializeResult(asset_key="vertexai_asset")

    result = materialize([vertexai_asset], resources=VERTEX_AI_RESOURCE_CONFIG)
    assert result.success
    mats = result.asset_materializations_for_node("vertexai_asset")
    assert len(mats) == 1
    mat = mats[0]

    assert mat.metadata["vertexai.calls"].value == 1
    # Fixed: Cast to int to satisfy pyright
    prompt_tokens = mat.metadata["vertexai.prompt_token_count"].value
    candidates_tokens = mat.metadata["vertexai.candidates_token_count"].value
    total_tokens = mat.metadata["vertexai.total_token_count"].value

    assert isinstance(prompt_tokens, int) and prompt_tokens > 0
    assert isinstance(candidates_tokens, int) and candidates_tokens > 0
    assert isinstance(total_tokens, int) and total_tokens > 0

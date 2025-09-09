from unittest.mock import MagicMock, patch

import pytest
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetSpec,
    Definitions,
    MaterializeResult,
    OpExecutionContext,
    StaticPartitionsDefinition,
    asset,
    build_asset_context,
    build_init_resource_context,
    build_op_context,
    graph_asset,
    job,
    materialize,
    multi_asset,
    op,
)
from dagster._core.errors import DagsterInvariantViolationError
from vertexai.generative_models import GenerationResponse

from dagster_vertexai import VertexAIResource

# Constants for Vertex AI configuration
PROJECT_ID = "test-project-1234"
LOCATION = "us-central1"
MODEL_NAME = "gemini-1.5-flash-001"


@patch("vertexai.generative_models.GenerativeModel")
@patch("vertexai.init")
def test_vertexai_client(mock_init, mock_model_class) -> None:
    """Tests that the resource calls vertexai.init and initializes the model on setup."""
    vertexai_resource = VertexAIResource(
        project_id=PROJECT_ID, location=LOCATION, generative_model_name=MODEL_NAME
    )
    context = build_init_resource_context()
    vertexai_resource.setup_for_execution(context)

    mock_init.assert_called_once_with(
        project=PROJECT_ID, location=LOCATION, credentials=None, api_endpoint=None
    )
    mock_model_class.assert_called_once_with(model_name=MODEL_NAME)


@patch("vertexai.generative_models.GenerativeModel")
@patch("vertexai.init")
def test_vertexai_resource_with_op(mock_init, mock_model_class):
    """Tests that the resource can be used within an op."""

    @op
    def vertexai_op(vertexai: VertexAIResource):
        context = build_op_context(resources={"vertexai": vertexai})
        with vertexai.get_model(context=context) as model:
            assert model is not None

    vertexai_job = job(name="vertexai_op_job")(vertexai_op)

    defs = Definitions(
        jobs=[vertexai_job],
        resources={
            "vertexai": VertexAIResource(
                project_id=PROJECT_ID,
                location=LOCATION,
                generative_model_name=MODEL_NAME,
            )
        },
    )
    result = defs.get_job_def("vertexai_op_job").execute_in_process()
    assert result.success
    mock_init.assert_called_once()
    mock_model_class.assert_called_once()


@patch("vertexai.generative_models.GenerativeModel")
@patch("vertexai.init")
def test_vertexai_resource_with_asset(mock_init, mock_model_class):
    """Tests that the resource can be used within an asset."""

    @asset
    def vertexai_asset(vertexai: VertexAIResource):
        context = build_asset_context(resources={"vertexai": vertexai})
        with vertexai.get_model(context=context) as model:
            assert model is not None

    result = materialize(
        [vertexai_asset],
        resources={
            "vertexai": VertexAIResource(
                project_id=PROJECT_ID,
                location=LOCATION,
                generative_model_name=MODEL_NAME,
            )
        },
    )
    assert result.success


@patch("vertexai.generative_models.GenerativeModel")
@patch("vertexai.init")
def test_vertexai_resource_with_graph_backed_asset(mock_init, mock_model_class):
    """Tests resource usage in a graph-backed asset, ensuring the wrapper is called."""
    mock_model_instance = mock_model_class.return_value
    mock_model_instance.generate_content.return_value = MagicMock(
        spec=GenerationResponse
    )

    @op
    def message_op():
        return "Say this is a test"

    @op
    def vertexai_op(
        context: OpExecutionContext, vertexai: VertexAIResource, message: str
    ):
        with vertexai.get_model(context=context) as model:
            model.generate_content(message)

    @graph_asset
    def vertexai_asset(vertexai: VertexAIResource):
        return vertexai_op(vertexai, message_op())

    with patch(
        "dagster_vertexai.resources.VertexAIResource._wrap_for_usage_tracking"
    ) as mock_wrapper:
        result = materialize(
            [vertexai_asset],
            resources={
                "vertexai": VertexAIResource(
                    project_id=PROJECT_ID,
                    location=LOCATION,
                    generative_model_name=MODEL_NAME,
                )
            },
        )
        assert result.success
        mock_wrapper.assert_called()


@patch("vertexai.generative_models.GenerativeModel")
@patch("vertexai.init")
def test_vertexai_resource_with_multi_asset(mock_init, mock_model_class):
    """Tests multi-asset scenarios: success with get_model_for_asset, failure with get_model."""

    @multi_asset(
        specs=[AssetSpec("status"), AssetSpec("result")],
    )
    def vertexai_multi_asset(
        context: AssetExecutionContext, vertexai: VertexAIResource
    ):
        with vertexai.get_model_for_asset(
            context=context, asset_key=AssetKey("result")
        ) as model:
            model.generate_content("Say this is a test")

        with pytest.raises(DagsterInvariantViolationError):
            with vertexai.get_model(context=context):
                pass

    with patch(
        "dagster_vertexai.resources.VertexAIResource._wrap_for_usage_tracking"
    ) as mock_wrapper:
        result = materialize(
            [vertexai_multi_asset],
            resources={
                "vertexai": VertexAIResource(
                    project_id=PROJECT_ID,
                    location=LOCATION,
                    generative_model_name=MODEL_NAME,
                )
            },
        )
        assert result.success
        mock_wrapper.assert_called_once()
        call_args = mock_wrapper.call_args[0]
        assert call_args[1] == "result"


@patch("vertexai.generative_models.GenerativeModel")
@patch("vertexai.init")
def test_vertexai_wrapper_with_asset(mock_init, mock_model_class):
    """Tests that the metadata wrapper correctly logs usage for a simple asset."""
    mock_response = MagicMock(spec=GenerationResponse)
    # This is the correct, robust way to test: mock the interface, not the implementation.
    # We create an object that has the attributes our code needs, without importing a private class.
    mock_usage = MagicMock()
    mock_usage.prompt_token_count = 1
    mock_usage.candidates_token_count = 1
    mock_usage.total_token_count = 2
    mock_response.usage_metadata = mock_usage
    mock_model_class.return_value.generate_content.return_value = mock_response

    @asset
    def vertexai_asset(vertexai: VertexAIResource):
        context = build_asset_context(resources={"vertexai": vertexai})
        with vertexai.get_model(context=context) as model:
            model.generate_content("say this is a test")
        return MaterializeResult(asset_key="vertexai_asset")

    result = materialize(
        [vertexai_asset],
        resources={
            "vertexai": VertexAIResource(
                project_id=PROJECT_ID,
                location=LOCATION,
                generative_model_name=MODEL_NAME,
            )
        },
    )
    assert result.success
    mats = result.asset_materializations_for_node("vertexai_asset")
    assert len(mats) == 1
    mat = mats[0]
    assert mat.metadata["vertexai.calls"].value == 1
    assert mat.metadata["vertexai.total_token_count"].value == 2


@patch("vertexai.generative_models.GenerativeModel")
@patch("vertexai.init")
def test_vertexai_wrapper_with_op(mock_init, mock_model_class):
    """Tests that usage metadata is NOT logged when used in an op context."""
    mock_response = MagicMock(spec=GenerationResponse)
    mock_usage = MagicMock()
    mock_usage.prompt_token_count = 1
    mock_usage.total_token_count = 1
    mock_response.usage_metadata = mock_usage
    mock_model_class.return_value.generate_content.return_value = mock_response

    @op
    def vertexai_op(vertexai: VertexAIResource):
        context = build_op_context(resources={"vertexai": vertexai})
        context.add_output_metadata = MagicMock()
        with vertexai.get_model(context=context) as model:
            model.generate_content("Say this is a test")
        context.add_output_metadata.assert_not_called()

    @job
    def vertexai_job():
        vertexai_op()

    result = vertexai_job.execute_in_process(
        resources={
            "vertexai": VertexAIResource(
                project_id=PROJECT_ID,
                location=LOCATION,
                generative_model_name=MODEL_NAME,
            )
        }
    )
    assert result.success


@patch("vertexai.generative_models.GenerativeModel")
@patch("vertexai.init")
def test_vertexai_wrapper_with_partitioned_asset(mock_init, mock_model_class):
    """Tests metadata logging for a partitioned asset."""
    mock_response = MagicMock(spec=GenerationResponse)
    mock_usage = MagicMock()
    mock_usage.prompt_token_count = 10
    mock_usage.total_token_count = 10
    mock_response.usage_metadata = mock_usage
    mock_model_class.return_value.generate_content.return_value = mock_response

    partitions_def = StaticPartitionsDefinition(["a", "b"])

    @asset(partitions_def=partitions_def)
    def vertexai_partitioned_asset(
        context: AssetExecutionContext, vertexai: VertexAIResource
    ):
        with vertexai.get_model(context=context) as model:
            model.generate_content(f"data for {context.partition_key}")
        return MaterializeResult()

    result = materialize(
        [vertexai_partitioned_asset],
        partition_key="a",
        resources={
            "vertexai": VertexAIResource(
                project_id=PROJECT_ID,
                location=LOCATION,
                generative_model_name=MODEL_NAME,
            )
        },
    )
    assert result.success
    mats = result.asset_materializations_for_node("vertexai_partitioned_asset")
    assert len(mats) == 1
    mat = mats[0]
    assert mat.metadata["vertexai.calls"].value == 1
    assert mat.metadata["vertexai.total_token_count"].value == 10

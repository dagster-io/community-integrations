from unittest.mock import MagicMock, patch

import pytest
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetSelection,
    AssetSpec,
    Definitions,
    OpExecutionContext,
    StaticPartitionsDefinition,
    asset,
    define_asset_job,
    graph_asset,
    materialize_to_memory,
    multi_asset,
    op,
    job,
)
from dagster._core.errors import DagsterInvariantViolationError
from dagster._core.execution.context.init import build_init_resource_context
from dagster._utils.test import wrap_op_in_graph_and_execute
from dagster_gemini import GeminiResource

API_KEY = "xoxp-1234123412341234-12341234-1234"
MODEL_NAME = "gemini-1.5-flash"


@patch("dagster_gemini.resource.GenerativeModel")
@patch("dagster_gemini.resource.genai.configure")
def test_gemini_client(mock_configure, mock_model) -> None:
    gemini_resource = GeminiResource(api_key=API_KEY, generative_model_name=MODEL_NAME)
    gemini_resource.setup_for_execution(build_init_resource_context())

    mock_context = MagicMock()
    with gemini_resource.get_model(mock_context):
        mock_configure.assert_called_once_with(
            api_key=API_KEY,
        )
        mock_model.assert_called_once_with(model_name=MODEL_NAME)


@patch("dagster_gemini.resource.GenerativeModel")
@patch("dagster.OpExecutionContext", autospec=OpExecutionContext)
@patch("dagster_gemini.resource.genai.configure")
def test_gemini_resource_with_op(mock_configure, mock_context, mock_model):
    @op
    def gemini_op(gemini_resource: GeminiResource):
        with gemini_resource.get_model(context=mock_context):
            mock_configure.assert_called_once_with(api_key=API_KEY)
            mock_model.assert_called_once_with(model_name=MODEL_NAME)

    result = wrap_op_in_graph_and_execute(
        gemini_op,
        resources={
            "gemini_resource": GeminiResource(
                api_key=API_KEY, generative_model_name=MODEL_NAME
            )
        },
    )
    assert result.success


@patch("dagster_gemini.resource.GenerativeModel")
@patch("dagster.AssetExecutionContext", autospec=AssetExecutionContext)
@patch("dagster_gemini.resource.genai.configure")
def test_gemini_resource_with_asset(mock_configure, mock_context, mock_model):
    @asset
    def gemini_asset(gemini_resource: GeminiResource):
        with gemini_resource.get_model(context=mock_context):
            mock_configure.assert_called_once_with(api_key=API_KEY)
            mock_model.assert_called_once_with(model_name=MODEL_NAME)

    result = materialize_to_memory(
        [gemini_asset],
        resources={
            "gemini_resource": GeminiResource(
                api_key=API_KEY, generative_model_name=MODEL_NAME
            )
        },
    )

    assert result.success


@patch("dagster_gemini.resource.GeminiResource._wrap_with_usage_metadata")
@patch("dagster_gemini.resource.GenerativeModel")
@patch("dagster.AssetExecutionContext", autospec=AssetExecutionContext)
@patch("dagster_gemini.resource.genai.configure")
def test_gemini_resource_with_graph_backed_asset(
    mock_configure, mock_context, mock_model, mock_wrapper
):
    @op
    def message_op():
        return "Say this is a test"

    @op
    def gemini_op(gemini_resource: GeminiResource, message: str):
        with gemini_resource.get_model(context=mock_context) as model:
            model.generate_content(message)

        assert mock_model.called
        assert mock_wrapper.called

    @graph_asset
    def gemini_asset():
        return gemini_op(message_op())

    result = materialize_to_memory(
        [gemini_asset],
        resources={
            "gemini_resource": GeminiResource(
                api_key=API_KEY, generative_model_name=MODEL_NAME
            )
        },
    )

    assert result.success


@patch("dagster_gemini.resource.GeminiResource._wrap_with_usage_metadata")
@patch("dagster_gemini.resource.GenerativeModel")
@patch("dagster.AssetExecutionContext", autospec=AssetExecutionContext)
@patch("dagster_gemini.resource.genai.configure")
def test_gemini_resource_with_multi_asset(
    mock_configure, mock_context, mock_model, mock_wrapper
):
    @multi_asset(
        specs=[AssetSpec("status"), AssetSpec("result")],
    )
    def gemini_multi_asset(gemini_resource: GeminiResource):
        mock_context.assets_def.keys_by_output_name.keys.return_value = [
            AssetKey("status"),
            AssetKey("result"),
        ]
        mock_context.output_for_asset_key.return_value = "result"

        # Test success when asset_key is provided
        with gemini_resource.get_model_for_asset(
            context=mock_context, asset_key=AssetKey("result")
        ) as model:
            model.generate_content("Say this is a test")

        assert mock_model.called
        assert mock_wrapper.call_count == 1
        mock_wrapper.assert_called_with(
            context=mock_context,
            output_name="result",
        )

        # Test failure when asset_key is not provided
        with pytest.raises(DagsterInvariantViolationError):
            with gemini_resource.get_model(context=mock_context) as model:
                model.generate_content("Say this is a test")
        return None, None

    result = materialize_to_memory(
        [gemini_multi_asset],
        resources={
            "gemini_resource": GeminiResource(
                api_key=API_KEY, generative_model_name=MODEL_NAME
            )
        },
    )

    assert result.success


@patch("dagster_gemini.resource.GeminiResource._wrap_with_usage_metadata")
@patch("dagster.AssetExecutionContext", autospec=AssetExecutionContext)
@patch("dagster_gemini.resource.GenerativeModel")
@patch("dagster_gemini.resource.genai.configure")
def test_gemini_resource_with_partitioned_asset(
    mock_configure, mock_model, mock_context, mock_wrapper
):
    NUM_PARTITION_KEYS = 4
    NUM_ASSET_DEFS = 5

    gemini_partitions_def = StaticPartitionsDefinition(
        [str(j) for j in range(NUM_PARTITION_KEYS)]
    )

    gemini_partitioned_assets = []

    for i in range(NUM_ASSET_DEFS):

        @asset(
            name=f"gemini_partitioned_asset_{i}",
            group_name="gemini_partitioned_assets",
            partitions_def=gemini_partitions_def,
        )
        def gemini_partitioned_asset(gemini_resource: GeminiResource):
            assert gemini_resource

            mock_context.output_for_asset_key.return_value = "test"

            with gemini_resource.get_model(context=mock_context) as model:
                model.generate_content("Say this is a test")

            assert mock_model.called
            assert mock_wrapper.called
            mock_wrapper.assert_called_with(
                context=mock_context,
                output_name="test",
            )

        gemini_partitioned_assets.append(gemini_partitioned_asset)

    defs = Definitions(
        assets=gemini_partitioned_assets,
        jobs=[
            define_asset_job(
                name="gemini_partitioned_asset_job",
                selection=AssetSelection.groups("gemini_partitioned_assets"),
                partitions_def=gemini_partitions_def,
            )
        ],
        resources={
            "gemini_resource": GeminiResource(
                api_key=API_KEY, generative_model_name=MODEL_NAME
            )
        },
    )

    for partition_key in gemini_partitions_def.get_partition_keys():
        result = defs.get_job_def("gemini_partitioned_asset_job").execute_in_process(
            partition_key=partition_key
        )
        assert result.success

    expected_wrapper_call_counts = len(gemini_partitioned_assets) * len(
        gemini_partitions_def.get_partition_keys()
    )
    assert mock_wrapper.call_count == expected_wrapper_call_counts


@patch("dagster.AssetExecutionContext", autospec=AssetExecutionContext)
@patch("dagster_gemini.resource.GenerativeModel")
@patch("dagster_gemini.resource.genai.configure")
def test_gemini_wrapper_with_asset(mock_configure, mock_model, mock_context):
    @asset
    def gemini_asset(gemini_resource: GeminiResource):
        mock_context.assets_def.keys_by_output_name.keys.return_value = [
            AssetKey("gemini_asset"),
        ]
        mock_context.asset_key = AssetKey("gemini_asset")
        mock_context.output_for_asset_key.return_value = "gemini_asset"

        mock_response = MagicMock()
        mock_usage = MagicMock()

        mock_usage.cached_content_token_count = 0
        mock_usage.prompt_token_count = 1
        mock_usage.candidates_token_count = 1
        mock_usage.total_token_count = 2

        mock_response.usage_metadata = mock_usage
        mock_model.return_value.generate_content.return_value = mock_response

        with gemini_resource.get_model(context=mock_context) as model:
            model.generate_content("say this is a test")

            mock_context.add_output_metadata.assert_called_with(
                metadata={
                    "gemini.calls": 1,
                    "gemini.cached_content_token_count": 0,
                    "gemini.candidates_token_count": 1,
                    "gemini.prompt_token_count": 1,
                    "gemini.total_token_count": 2,
                },
                output_name="gemini_asset",
            )

    result = materialize_to_memory(
        [gemini_asset],
        resources={
            "gemini_resource": GeminiResource(
                api_key=API_KEY, generative_model_name=MODEL_NAME
            )
        },
    )

    assert result.success


@patch("dagster.AssetExecutionContext", autospec=AssetExecutionContext)
@patch("dagster_gemini.resource.GenerativeModel")
@patch("dagster_gemini.resource.genai.configure")
def test_gemini_wrapper_with_graph_backed_asset(
    mock_configure, mock_model, mock_context
):
    @op
    def message_op():
        return "Say this is a test"

    @op
    def gemini_op(gemini_resource: GeminiResource, message: str):
        mock_context.assets_def.keys_by_output_name.keys.return_value = [
            AssetKey("gemini_asset"),
        ]
        mock_context.asset_key = AssetKey("gemini_asset")
        mock_context.output_for_asset_key.return_value = "gemini_asset"

        mock_response = MagicMock()
        mock_usage = MagicMock()

        mock_usage.cached_content_token_count = 0
        mock_usage.prompt_token_count = 1
        mock_usage.candidates_token_count = 1
        mock_usage.total_token_count = 2

        mock_response.usage_metadata = mock_usage
        mock_model.return_value.generate_content.return_value = mock_response

        with gemini_resource.get_model(context=mock_context) as model:
            model.generate_content(message)

            mock_context.add_output_metadata.assert_called_with(
                metadata={
                    "gemini.calls": 1,
                    "gemini.cached_content_token_count": 0,
                    "gemini.candidates_token_count": 1,
                    "gemini.prompt_token_count": 1,
                    "gemini.total_token_count": 2,
                },
                output_name="gemini_asset",
            )

    @graph_asset
    def gemini_asset():
        return gemini_op(message_op())

    result = materialize_to_memory(
        [gemini_asset],
        resources={
            "gemini_resource": GeminiResource(
                api_key=API_KEY, generative_model_name=MODEL_NAME
            )
        },
    )

    assert result.success


# Test that usage metadata is not logged in an op context
@patch("dagster.OpExecutionContext", autospec=OpExecutionContext)
@patch("dagster_gemini.resource.GenerativeModel")
@patch("dagster_gemini.resource.genai.configure")
def test_gemini_wrapper_with_op(mock_configure, mock_model, mock_context):
    @op
    def gemini_op(gemini_resource: GeminiResource):
        mock_context.assets_def.keys_by_output_name.keys.return_value = [
            AssetKey("gemini_op"),
        ]
        mock_context.asset_key = AssetKey("gemini_op")
        mock_context.output_for_asset_key.return_value = "gemini_op"

        mock_response = MagicMock()
        mock_usage = MagicMock()

        mock_usage.cached_content_token_count = 0
        mock_usage.prompt_token_count = 1
        mock_usage.candidates_token_count = 1
        mock_usage.total_token_count = 2

        mock_response.usage_metadata = mock_usage
        mock_model.return_value.generate_content.return_value = mock_response

        with gemini_resource.get_model(context=mock_context) as model:
            model.generate_content("Say this is a test")

            assert not mock_context.add_output_metadata.called

    @job
    def gemini_job():
        gemini_op()

    defs = Definitions(
        jobs=[gemini_job],
        resources={
            "gemini_resource": GeminiResource(
                api_key=API_KEY, generative_model_name=MODEL_NAME
            )
        },
    )

    result = defs.get_job_def("gemini_job").execute_in_process()
    assert result.success


@patch("dagster.AssetExecutionContext", autospec=AssetExecutionContext)
@patch("dagster_gemini.resource.GenerativeModel")
@patch("dagster_gemini.resource.genai.configure")
def test_gemini_wrapper_with_multi_asset(mock_configure, mock_model, mock_context):
    @multi_asset(
        specs=[AssetSpec("status"), AssetSpec("result")],
    )
    def gemini_multi_asset(gemini_resource: GeminiResource):
        mock_context.assets_def.keys_by_output_name.keys.return_value = [
            AssetKey("status"),
            AssetKey("result"),
        ]
        mock_context.output_for_asset_key.return_value = "result"

        mock_response = MagicMock()
        mock_usage = MagicMock()

        mock_usage.cached_content_token_count = 0
        mock_usage.prompt_token_count = 1
        mock_usage.candidates_token_count = 1
        mock_usage.total_token_count = 2

        mock_response.usage_metadata = mock_usage
        mock_model.return_value.generate_content.return_value = mock_response

        with gemini_resource.get_model_for_asset(
            context=mock_context, asset_key=AssetKey("result")
        ) as model:
            model.generate_content("Say this is a test")

            mock_context.add_output_metadata.assert_called_with(
                metadata={
                    "gemini.calls": 1,
                    "gemini.cached_content_token_count": 0,
                    "gemini.candidates_token_count": 1,
                    "gemini.prompt_token_count": 1,
                    "gemini.total_token_count": 2,
                },
                output_name="result",
            )
        return None, None

    result = materialize_to_memory(
        [gemini_multi_asset],
        resources={
            "gemini_resource": GeminiResource(
                api_key=API_KEY, generative_model_name=MODEL_NAME
            )
        },
    )

    assert result.success


@patch("dagster_gemini.resource.GenerativeModel")
@patch("dagster_gemini.resource.genai.configure")
def test_gemini_wrapper_with_partitioned_asset(mock_configure, mock_model):
    NUM_PARTITION_KEYS = 4
    NUM_ASSET_DEFS = 5

    gemini_partitions_def = StaticPartitionsDefinition(
        [str(j) for j in range(NUM_PARTITION_KEYS)]
    )

    gemini_partitioned_assets = []

    for i in range(NUM_ASSET_DEFS):

        @asset(
            name=f"gemini_partitioned_asset_{i}",
            group_name="gemini_partitioned_assets",
            partitions_def=gemini_partitions_def,
        )
        def gemini_partitioned_asset(gemini_resource: GeminiResource):
            mock_context = MagicMock(autospec=AssetExecutionContext)
            mock_context.__class__ = AssetExecutionContext
            mock_context.assets_def.keys_by_output_name.keys.return_value = [
                AssetKey(f"gemini_partitioned_asset_{i}"),
            ]
            mock_context.asset_key = AssetKey(f"gemini_partitioned_asset_{i}")
            mock_context.output_for_asset_key.return_value = (
                f"gemini_partitioned_asset_{i}"
            )

            mock_response = MagicMock()
            mock_usage = MagicMock()

            mock_usage.cached_content_token_count = 0
            mock_usage.prompt_token_count = 1
            mock_usage.candidates_token_count = 1
            mock_usage.total_token_count = 2

            mock_response.usage_metadata = mock_usage
            mock_model.return_value.generate_content.return_value = mock_response

            with gemini_resource.get_model(context=mock_context) as model:
                model.generate_content("Say this is a test")

                mock_context.add_output_metadata.assert_called_with(
                    {
                        "gemini.calls": 1,
                        "gemini.cached_content_token_count": 0,
                        "gemini.candidates_token_count": 1,
                        "gemini.prompt_token_count": 1,
                        "gemini.total_token_count": 2,
                    },
                    f"gemini_partitioned_asset_{i}",
                )

        gemini_partitioned_assets.append(gemini_partitioned_asset)

    defs = Definitions(
        assets=gemini_partitioned_assets,
        jobs=[
            define_asset_job(
                name="gemini_partitioned_asset_job",
                selection=AssetSelection.groups("gemini_partitioned_assets"),
                partitions_def=gemini_partitions_def,
            )
        ],
        resources={
            "gemini_resource": GeminiResource(
                api_key=API_KEY, generative_model_name=MODEL_NAME
            )
        },
    )

    for partition_key in gemini_partitions_def.get_partition_keys():
        result = defs.get_job_def("gemini_partitioned_asset_job").execute_in_process(
            partition_key=partition_key
        )
        assert result.success

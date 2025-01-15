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
from dagster_anthropic import AnthropicResource

API_KEY = "xoxp-1234123412341234-12341234-1234"
MODEL = "claude-3-5-sonnet-20241022"


@patch("dagster_anthropic.resource.Anthropic")
def test_anthropic_client(mock_client) -> None:
    anthropic_resource = AnthropicResource(api_key=API_KEY)
    anthropic_resource.setup_for_execution(build_init_resource_context())

    mock_context = MagicMock()
    with anthropic_resource.get_client(mock_context):
        mock_client.assert_called_once_with(
            api_key=API_KEY,
            base_url=None,
        )


@patch("dagster_anthropic.resource.Anthropic")
def test_anthropic_client_with_config(mock_client) -> None:
    anthropic_resource = AnthropicResource(
        api_key=API_KEY,
        base_url="http://foo.bar",
    )
    anthropic_resource.setup_for_execution(build_init_resource_context())

    mock_context = MagicMock()
    with anthropic_resource.get_client(mock_context):
        mock_client.assert_called_once_with(
            api_key=API_KEY,
            base_url="http://foo.bar",
        )


@patch("dagster_anthropic.resource.AnthropicResource._wrap_with_usage_metadata")
@patch("dagster.OpExecutionContext", autospec=OpExecutionContext)
@patch("dagster_anthropic.resource.Anthropic")
def test_anthropic_resource_with_op(mock_client, mock_context, mock_wrapper):
    @op
    def anthropic_op(anthropic_resource: AnthropicResource):
        with anthropic_resource.get_client(context=mock_context) as client:
            client.messages.create(
                model=MODEL,
                max_tokens=1024,
                messages=[{"role": "user", "content": "Say this is a test"}],
            )

        assert mock_client.called
        assert not mock_wrapper.called

    result = wrap_op_in_graph_and_execute(
        anthropic_op,
        resources={"anthropic_resource": AnthropicResource(api_key=API_KEY)},
    )
    assert result.success


@patch("dagster_anthropic.resource.AnthropicResource._wrap_with_usage_metadata")
@patch("dagster.AssetExecutionContext", autospec=AssetExecutionContext)
@patch("dagster_anthropic.resource.Anthropic")
def test_anthropic_resource_with_asset(mock_client, mock_context, mock_wrapper):
    @asset
    def anthropic_asset(anthropic_resource: AnthropicResource):
        assert anthropic_resource

        with anthropic_resource.get_client(context=mock_context) as client:
            client.messages.create(
                model=MODEL,
                max_tokens=1024,
                messages=[{"role": "user", "content": "Say this is a test"}],
            )

        assert mock_client.called
        assert mock_wrapper.call_count == 1

    result = materialize_to_memory(
        [anthropic_asset],
        resources={"anthropic_resource": AnthropicResource(api_key=API_KEY)},
    )

    assert result.success


@patch("dagster_anthropic.resource.AnthropicResource._wrap_with_usage_metadata")
@patch("dagster.AssetExecutionContext", autospec=AssetExecutionContext)
@patch("dagster_anthropic.resource.Anthropic")
def test_anthropic_resource_with_graph_backed_asset(
    mock_client, mock_context, mock_wrapper
):
    @op
    def model_version_op():
        return MODEL

    @op
    def message_op():
        return {"role": "user", "content": "Say this is a test"}

    @op
    def anthropic_op(anthropic_resource: AnthropicResource, model_version, message):
        with anthropic_resource.get_client(context=mock_context) as client:
            client.messages.create(
                model=model_version, max_tokens=1024, messages=[message]
            )

        assert mock_client.called
        assert mock_wrapper.called

    @graph_asset
    def anthropic_asset():
        return anthropic_op(model_version_op(), message_op())

    result = materialize_to_memory(
        [anthropic_asset],
        resources={"anthropic_resource": AnthropicResource(api_key=API_KEY)},
    )

    assert result.success


@patch("dagster_anthropic.resource.AnthropicResource._wrap_with_usage_metadata")
@patch("dagster.AssetExecutionContext", autospec=AssetExecutionContext)
@patch("dagster_anthropic.resource.Anthropic")
def test_anthropic_resource_with_multi_asset(mock_client, mock_context, mock_wrapper):
    @multi_asset(
        specs=[AssetSpec("status"), AssetSpec("result")],
    )
    def anthropic_multi_asset(anthropic_resource: AnthropicResource):
        mock_context.assets_def.keys_by_output_name.keys.return_value = [
            AssetKey("status"),
            AssetKey("result"),
        ]
        mock_context.output_for_asset_key.return_value = "result"

        # Test success when asset_key is provided
        with anthropic_resource.get_client_for_asset(
            context=mock_context, asset_key=AssetKey("result")
        ) as client:
            client.messages.create(
                model=MODEL,
                max_tokens=1024,
                messages=[{"role": "user", "content": "Say this is a test"}],
            )

        assert mock_client.called
        assert mock_wrapper.call_count == 1
        mock_wrapper.assert_called_with(
            context=mock_context,
            output_name="result",
        )

        # Test failure when asset_key is not provided
        with pytest.raises(DagsterInvariantViolationError):
            with anthropic_resource.get_client(context=mock_context) as client:
                client.messages.create(
                    model=MODEL,
                    max_tokens=1024,
                    messages=[{"role": "user", "content": "Say this is a test"}],
                )
        return None, None

    result = materialize_to_memory(
        [anthropic_multi_asset],
        resources={"anthropic_resource": AnthropicResource(api_key=API_KEY)},
    )

    assert result.success


@patch("dagster_anthropic.resource.AnthropicResource._wrap_with_usage_metadata")
@patch("dagster.AssetExecutionContext", autospec=AssetExecutionContext)
@patch("dagster_anthropic.resource.Anthropic")
def test_anthropic_resource_with_partitioned_asset(
    mock_client, mock_context, mock_wrapper
):
    NUM_PARTITION_KEYS = 4
    NUM_ASSET_DEFS = 5

    anthropic_partitions_def = StaticPartitionsDefinition(
        [str(j) for j in range(NUM_PARTITION_KEYS)]
    )

    anthropic_partitioned_assets = []

    for i in range(NUM_ASSET_DEFS):

        @asset(
            name=f"anthropic_partitioned_asset_{i}",
            group_name="anthropic_partitioned_assets",
            partitions_def=anthropic_partitions_def,
        )
        def anthropic_partitioned_asset(anthropic_resource: AnthropicResource):
            assert anthropic_resource

            mock_context.output_for_asset_key.return_value = "test"

            with anthropic_resource.get_client(context=mock_context) as client:
                client.messages.create(
                    model=MODEL,
                    max_tokens=1024,
                    messages=[{"role": "user", "content": "Say this is a test"}],
                )

            assert mock_client.called
            assert mock_wrapper.called
            mock_wrapper.assert_called_with(
                context=mock_context,
                output_name="test",
            )

        anthropic_partitioned_assets.append(anthropic_partitioned_asset)

    defs = Definitions(
        assets=anthropic_partitioned_assets,
        jobs=[
            define_asset_job(
                name="anthropic_partitioned_asset_job",
                selection=AssetSelection.groups("anthropic_partitioned_assets"),
                partitions_def=anthropic_partitions_def,
            )
        ],
        resources={"anthropic_resource": AnthropicResource(api_key=API_KEY)},
    )

    for partition_key in anthropic_partitions_def.get_partition_keys():
        result = defs.get_job_def("anthropic_partitioned_asset_job").execute_in_process(
            partition_key=partition_key
        )
        assert result.success

    expected_wrapper_call_counts = len(anthropic_partitioned_assets) * len(
        anthropic_partitions_def.get_partition_keys()
    )
    assert mock_wrapper.call_count == expected_wrapper_call_counts


@patch("dagster.AssetExecutionContext", autospec=AssetExecutionContext)
@patch("dagster_anthropic.resource.Anthropic")
def test_anthropic_wrapper_with_asset(mock_client, mock_context):
    @asset
    def anthropic_asset(anthropic_resource: AnthropicResource):
        mock_context.assets_def.keys_by_output_name.keys.return_value = [
            AssetKey("anthropic_asset"),
        ]
        mock_context.asset_key = AssetKey("anthropic_asset")
        mock_context.output_for_asset_key.return_value = "anthropic_asset"

        mock_message = MagicMock()
        mock_usage = MagicMock()

        mock_usage.input_tokens = 1
        mock_usage.output_tokens = 1
        mock_usage.cache_creation_input_tokens = None
        mock_usage.cache_read_input_tokens = None

        mock_message.usage = mock_usage
        mock_client.return_value.messages.create.return_value = mock_message

        with anthropic_resource.get_client(context=mock_context) as client:
            client.messages.create(
                model=MODEL,
                max_tokens=1024,
                messages=[{"role": "user", "content": "Say this is a test"}],
            )

            mock_context.add_output_metadata.assert_called_with(
                metadata={
                    "anthropic.calls": 1,
                    "anthropic.input_tokens": 1,
                    "anthropic.output_tokens": 1,
                    "anthropic.cache_creation_input_tokens": 0,
                    "anthropic.cache_read_input_tokens": 0,
                },
                output_name="anthropic_asset",
            )

    result = materialize_to_memory(
        [anthropic_asset],
        resources={"anthropic_resource": AnthropicResource(api_key=API_KEY)},
    )

    assert result.success


@patch("dagster.AssetExecutionContext", autospec=AssetExecutionContext)
@patch("dagster_anthropic.resource.Anthropic")
def test_anthropic_wrapper_with_graph_backed_asset(mock_client, mock_context):
    @op
    def model_version_op():
        return MODEL

    @op
    def message_op():
        return {"role": "user", "content": "Say this is a test"}

    @op
    def anthropic_op(anthropic_resource: AnthropicResource, model_version, message):
        mock_context.assets_def.keys_by_output_name.keys.return_value = [
            AssetKey("anthropic_asset"),
        ]
        mock_context.asset_key = AssetKey("anthropic_asset")
        mock_context.output_for_asset_key.return_value = "anthropic_asset"

        mock_message = MagicMock()
        mock_usage = MagicMock()

        mock_usage.input_tokens = 1
        mock_usage.output_tokens = 1
        mock_usage.cache_creation_input_tokens = None
        mock_usage.cache_read_input_tokens = None

        mock_message.usage = mock_usage
        mock_client.return_value.messages.create.return_value = mock_message

        with anthropic_resource.get_client(context=mock_context) as client:
            client.messages.create(
                model=model_version,
                max_tokens=1024,
                messages=[message],
            )

            mock_context.add_output_metadata.assert_called_with(
                metadata={
                    "anthropic.calls": 1,
                    "anthropic.input_tokens": 1,
                    "anthropic.output_tokens": 1,
                    "anthropic.cache_creation_input_tokens": 0,
                    "anthropic.cache_read_input_tokens": 0,
                },
                output_name="anthropic_asset",
            )

    @graph_asset
    def anthropic_asset():
        return anthropic_op(model_version_op(), message_op())

    result = materialize_to_memory(
        [anthropic_asset],
        resources={"anthropic_resource": AnthropicResource(api_key=API_KEY)},
    )

    assert result.success


# Test that usage metadata is not logged in an op context
@patch("dagster.OpExecutionContext", autospec=OpExecutionContext)
@patch("dagster_anthropic.resource.Anthropic")
def test_anthropic_wrapper_with_op(mock_client, mock_context):
    @op
    def anthropic_op(anthropic_resource: AnthropicResource):
        mock_message = MagicMock()
        mock_usage = MagicMock()

        mock_usage.input_tokens = 1
        mock_usage.output_tokens = 1
        mock_usage.cache_creation_input_tokens = None
        mock_usage.cache_read_input_tokens = None

        mock_message.usage = mock_usage
        mock_client.return_value.messages.create.return_value = mock_message

        with anthropic_resource.get_client(context=mock_context) as client:
            client.messages.create(
                model=MODEL,
                max_tokens=1024,
                messages=[{"role": "user", "content": "Say this is a test"}],
            )

            assert not mock_context.add_output_metadata.called

    @job
    def anthropic_job():
        anthropic_op()

    defs = Definitions(
        jobs=[anthropic_job],
        resources={"anthropic_resource": AnthropicResource(api_key=API_KEY)},
    )

    result = defs.get_job_def("anthropic_job").execute_in_process()
    assert result.success


@patch("dagster.AssetExecutionContext", autospec=AssetExecutionContext)
@patch("dagster_anthropic.resource.Anthropic")
def test_anthropic_wrapper_with_multi_asset(mock_client, mock_context):
    @multi_asset(
        specs=[AssetSpec("status"), AssetSpec("result")],
    )
    def anthropic_multi_asset(anthropic_resource: AnthropicResource):
        mock_context.assets_def.keys_by_output_name.keys.return_value = [
            AssetKey("status"),
            AssetKey("result"),
        ]
        mock_context.output_for_asset_key.return_value = "result"

        mock_message = MagicMock()
        mock_usage = MagicMock()

        mock_usage.input_tokens = 1
        mock_usage.output_tokens = 1
        mock_usage.cache_creation_input_tokens = None
        mock_usage.cache_read_input_tokens = None

        mock_message.usage = mock_usage
        mock_client.return_value.messages.create.return_value = mock_message

        with anthropic_resource.get_client_for_asset(
            context=mock_context, asset_key=AssetKey("result")
        ) as client:
            client.messages.create(
                model=MODEL,
                max_tokens=1024,
                messages=[{"role": "user", "content": "Say this is a test"}],
            )

            mock_context.add_output_metadata.assert_called_with(
                metadata={
                    "anthropic.calls": 1,
                    "anthropic.input_tokens": 1,
                    "anthropic.output_tokens": 1,
                    "anthropic.cache_creation_input_tokens": 0,
                    "anthropic.cache_read_input_tokens": 0,
                },
                output_name="result",
            )
        return None, None

    result = materialize_to_memory(
        [anthropic_multi_asset],
        resources={"anthropic_resource": AnthropicResource(api_key=API_KEY)},
    )

    assert result.success


@patch("dagster_anthropic.resource.Anthropic")
def test_anthropic_wrapper_with_partitioned_asset(mock_client):
    NUM_PARTITION_KEYS = 4
    NUM_ASSET_DEFS = 5

    anthropic_partitions_def = StaticPartitionsDefinition(
        [str(j) for j in range(NUM_PARTITION_KEYS)]
    )

    anthropic_partitioned_assets = []

    for i in range(NUM_ASSET_DEFS):

        @asset(
            name=f"anthropic_partitioned_asset_{i}",
            group_name="anthropic_partitioned_assets",
            partitions_def=anthropic_partitions_def,
        )
        def anthropic_partitioned_asset(anthropic_resource: AnthropicResource):
            mock_context = MagicMock(autospec=AssetExecutionContext)
            mock_context.__class__ = AssetExecutionContext

            mock_context.assets_def.keys_by_output_name.keys.return_value = [
                AssetKey(f"anthropic_partitioned_asset_{i}"),
            ]
            mock_context.asset_key = AssetKey(f"anthropic_partitioned_asset_{i}")
            mock_context.output_for_asset_key.return_value = (
                f"anthropic_partitioned_asset_{i}"
            )

            mock_message = MagicMock()
            mock_usage = MagicMock()

            mock_usage.input_tokens = 1
            mock_usage.output_tokens = 1
            mock_usage.cache_creation_input_tokens = None
            mock_usage.cache_read_input_tokens = None

            mock_message.usage = mock_usage
            mock_client.return_value.messages.create.return_value = mock_message

            with anthropic_resource.get_client(context=mock_context) as client:
                client.messages.create(
                    model=MODEL,
                    max_tokens=1024,
                    messages=[{"role": "user", "content": "Say this is a test"}],
                )

                mock_context.add_output_metadata.assert_called_with(
                    {
                        "anthropic.calls": 1,
                        "anthropic.input_tokens": 1,
                        "anthropic.output_tokens": 1,
                        "anthropic.cache_creation_input_tokens": 0,
                        "anthropic.cache_read_input_tokens": 0,
                    },
                    f"anthropic_partitioned_asset_{i}",
                )

        anthropic_partitioned_assets.append(anthropic_partitioned_asset)

    defs = Definitions(
        assets=anthropic_partitioned_assets,
        jobs=[
            define_asset_job(
                name="anthropic_partitioned_asset_job",
                selection=AssetSelection.groups("anthropic_partitioned_assets"),
                partitions_def=anthropic_partitions_def,
            )
        ],
        resources={"anthropic_resource": AnthropicResource(api_key=API_KEY)},
    )

    for partition_key in anthropic_partitions_def.get_partition_keys():
        result = defs.get_job_def("anthropic_partitioned_asset_job").execute_in_process(
            partition_key=partition_key
        )
        assert result.success

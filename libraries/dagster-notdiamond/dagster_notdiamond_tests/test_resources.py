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
)
from dagster._core.errors import DagsterInvariantViolationError
from dagster._core.execution.context.init import build_init_resource_context
from dagster._utils.test import wrap_op_in_graph_and_execute
from dagster_notdiamond import NotDiamondResource, with_usage_metadata
from mock import ANY, MagicMock, patch


@patch("dagster_notdiamond.resources.NotDiamond")
def test_notdiamond_client(mock_client) -> None:
    notdiamond_resource = NotDiamondResource(api_key="xoxp-1234123412341234-12341234-1234")
    notdiamond_resource.setup_for_execution(build_init_resource_context())

    mock_context = MagicMock()
    with notdiamond_resource.get_client(mock_context):
        mock_client.assert_called_once_with(
            api_key="xoxp-1234123412341234-12341234-1234",
        )


@patch("dagster_notdiamond.resources.NotDiamond")
def test_notdiamond_client_with_config(mock_client) -> None:
    notdiamond_resource = NotDiamondResource(
        api_key="xoxp-1234123412341234-12341234-1234",
    )
    notdiamond_resource.setup_for_execution(build_init_resource_context())

    mock_context = MagicMock()
    with notdiamond_resource.get_client(mock_context):
        mock_client.assert_called_once_with(
            api_key="xoxp-1234123412341234-12341234-1234",
        )


@patch("dagster_notdiamond.resources.NotDiamondResource._wrap_with_usage_metadata")
@patch("dagster.OpExecutionContext", autospec=OpExecutionContext)
@patch("dagster_notdiamond.resources.NotDiamond")
def test_notdiamond_resource_with_op(mock_client, mock_context, mock_wrapper):
    @op
    def notdiamond_op(notdiamond_resource: NotDiamondResource):
        assert notdiamond_resource

        with notdiamond_resource.get_client(context=mock_context) as client:
            client.model_select(
                model=["openai/gpt-4o-mini", "openai/gpt-4o"],
                messages=[{"role": "user", "content": "Say this is a test"}],
            )

        assert mock_client.called
        assert mock_wrapper.not_called

    result = wrap_op_in_graph_and_execute(
        notdiamond_op,
        resources={
            "notdiamond_resource": NotDiamondResource(api_key="xoxp-1234123412341234-12341234-1234")
        },
    )
    assert result.success


@patch("dagster_notdiamond.resources.NotDiamondResource._wrap_with_usage_metadata")
@patch("dagster.AssetExecutionContext", autospec=AssetExecutionContext)
@patch("dagster_notdiamond.resources.NotDiamond")
def test_notdiamond_resource_with_asset(mock_client, mock_context, mock_wrapper):
    @asset
    def notdiamond_asset(notdiamond_resource: NotDiamondResource):
        assert notdiamond_resource

        with notdiamond_resource.get_client(context=mock_context) as client:
            client.model_select(
                model=["openai/gpt-4o-mini", "openai/gpt-4o"],
                messages=[{"role": "user", "content": "Say this is a test"}],
            )

        assert mock_client.called
        assert mock_wrapper.call_count == 1

    result = materialize_to_memory(
        [notdiamond_asset],
        resources={
            "notdiamond_resource": NotDiamondResource(api_key="xoxp-1234123412341234-12341234-1234")
        },
    )

    assert result.success


@patch("dagster_notdiamond.resources.NotDiamondResource._wrap_with_usage_metadata")
@patch("dagster.AssetExecutionContext", autospec=AssetExecutionContext)
@patch("dagster_notdiamond.resources.NotDiamond")
def test_notdiamond_resource_with_graph_backed_asset(mock_client, mock_context, mock_wrapper):
    @op
    def model_version_op():
        return ["openai/gpt-4o-mini", "openai/gpt-4o"]

    @op
    def message_op():
        return {"role": "user", "content": "Say this is a test"}

    @op
    def notdiamond_op(notdiamond_resource: NotDiamondResource, model_version, message):
        assert notdiamond_resource

        with notdiamond_resource.get_client(context=mock_context) as client:
            client.model_select(model=model_version, messages=[message])

        assert mock_client.called
        assert mock_wrapper.called

    @graph_asset
    def notdiamond_asset():
        return notdiamond_op(model_version_op(), message_op())

    result = materialize_to_memory(
        [notdiamond_asset],
        resources={
            "notdiamond_resource": NotDiamondResource(api_key="xoxp-1234123412341234-12341234-1234")
        },
    )

    assert result.success


@patch("dagster_notdiamond.resources.NotDiamondResource._wrap_with_usage_metadata")
@patch("dagster.AssetExecutionContext", autospec=AssetExecutionContext)
@patch("dagster_notdiamond.resources.NotDiamond")
def test_notdiamond_resource_with_multi_asset(mock_client, mock_context, mock_wrapper):
    @multi_asset(
        specs=[AssetSpec("status"), AssetSpec("result")],
    )
    def notdiamond_multi_asset(notdiamond_resource: NotDiamondResource):
        assert notdiamond_resource

        mock_context.assets_def.keys_by_output_name.keys.return_value = [
            AssetKey("status"),
            AssetKey("result"),
        ]
        mock_context.output_for_asset_key.return_value = "result"

        # Test success when asset_key is provided
        with notdiamond_resource.get_client_for_asset(
            context=mock_context, asset_key=AssetKey("result")
        ) as client:
            client.model_select(
                model=["openai/gpt-4o-mini", "openai/gpt-4o"],
                messages=[{"role": "user", "content": "Say this is a test"}],
            )

        assert mock_client.called
        assert mock_wrapper.call_count == 1
        mock_wrapper.assert_called_with(
            api_endpoint_class=ANY,
            context=mock_context,
            output_name="result",
        )

        # Test failure when asset_key is not provided
        with pytest.raises(DagsterInvariantViolationError):
            with notdiamond_resource.get_client(context=mock_context) as client:
                client.model_select(
                    model=["openai/gpt-4o-mini", "openai/gpt-4o"],
                    messages=[{"role": "user", "content": "Say this is a test"}],
                )
        return None, None

    result = materialize_to_memory(
        [notdiamond_multi_asset],
        resources={
            "notdiamond_resource": NotDiamondResource(api_key="xoxp-1234123412341234-12341234-1234")
        },
    )

    assert result.success


@patch("dagster_notdiamond.resources.NotDiamondResource._wrap_with_usage_metadata")
@patch("dagster.AssetExecutionContext", autospec=AssetExecutionContext)
@patch("dagster_notdiamond.resources.NotDiamond")
def test_notdiamond_resource_with_partitioned_asset(mock_client, mock_context, mock_wrapper):
    notdiamond_partitions_def = StaticPartitionsDefinition([str(j) for j in range(5)])

    notdiamond_partitioned_assets = []

    for i in range(5):

        @asset(
            name=f"notdiamond_partitioned_asset_{i}",
            group_name="notdiamond_partitioned_assets",
            partitions_def=notdiamond_partitions_def,
        )
        def notdiamond_partitioned_asset(notdiamond_resource: NotDiamondResource):
            assert notdiamond_resource

            mock_context.output_for_asset_key.return_value = "test"

            with notdiamond_resource.get_client(context=mock_context) as client:
                client.model_select(
                    model=["openai/gpt-4o-mini", "openai/gpt-4o"],
                    messages=[{"role": "user", "content": "Say this is a test"}],
                )

            assert mock_client.called
            assert mock_wrapper.called
            mock_wrapper.assert_called_with(
                api_endpoint_class=ANY,
                context=mock_context,
                output_name="test",
            )

        notdiamond_partitioned_assets.append(notdiamond_partitioned_asset)

    defs = Definitions(
        assets=notdiamond_partitioned_assets,
        jobs=[
            define_asset_job(
                name="notdiamond_partitioned_asset_job",
                selection=AssetSelection.groups("notdiamond_partitioned_assets"),
                partitions_def=notdiamond_partitions_def,
            )
        ],
        resources={
            "notdiamond_resource": NotDiamondResource(api_key="xoxp-1234123412341234-12341234-1234")
        },
    )

    for partition_key in notdiamond_partitions_def.get_partition_keys():
        result = defs.get_job_def("notdiamond_partitioned_asset_job").execute_in_process(
            partition_key=partition_key
        )
        assert result.success

    expected_wrapper_call_counts = (
        len(notdiamond_partitioned_assets) * len(notdiamond_partitions_def.get_partition_keys())
    )
    assert mock_wrapper.call_count == expected_wrapper_call_counts


@patch("dagster.OpExecutionContext", autospec=OpExecutionContext)
@patch("dagster_notdiamond.resources.NotDiamond")
def test_notdiamond_wrapper_with_op(mock_client, mock_context):
    @op
    def notdiamond_op(notdiamond_resource: NotDiamondResource):
        assert notdiamond_resource

        with notdiamond_resource.get_client(context=mock_context) as client:
            with pytest.raises(DagsterInvariantViolationError):
                client.fine_tuning.jobs.create = with_usage_metadata(
                    context=mock_context,
                    output_name="some_output_name",
                    func=client.fine_tuning.jobs.create,
                )

    result = wrap_op_in_graph_and_execute(
        notdiamond_op,
        resources={
            "notdiamond_resource": NotDiamondResource(api_key="xoxp-1234123412341234-12341234-1234")
        },
    )
    assert result.success


@patch("dagster_notdiamond.resources.NotDiamondResource._wrap_with_usage_metadata")
@patch("dagster.AssetExecutionContext", autospec=AssetExecutionContext)
@patch("dagster_notdiamond.resources.NotDiamond")
def test_notdiamond_wrapper_with_asset(mock_client, mock_context, mock_wrapper):
    @asset
    def notdiamond_asset(notdiamond_resource: NotDiamondResource):
        assert notdiamond_resource

        mock_completion = MagicMock()
        mock_usage = MagicMock()
        mock_usage.prompt_tokens = 1
        mock_usage.total_tokens = 1
        mock_usage.completion_tokens = 1
        mock_completion.usage = mock_usage
        mock_client.return_value.fine_tuning.jobs.create.return_value = mock_completion

        with notdiamond_resource.get_client(context=mock_context) as client:
            client.fine_tuning.jobs.create = with_usage_metadata(
                context=mock_context,
                output_name="notdiamond_asset",
                func=client.fine_tuning.jobs.create,
            )
            client.fine_tuning.jobs.create(
                model=["openai/gpt-4o-mini", "openai/gpt-4o"], training_file="some_training_file"
            )

            mock_context.add_output_metadata.assert_called_with(
                metadata={
                    "notdiamond.calls": 1,
                    "notdiamond.total_tokens": 1,
                    "notdiamond.prompt_tokens": 1,
                    "notdiamond.completion_tokens": 1,
                },
                output_name="notdiamond_asset",
            )

    result = materialize_to_memory(
        [notdiamond_asset],
        resources={
            "notdiamond_resource": NotDiamondResource(api_key="xoxp-1234123412341234-12341234-1234")
        },
    )

    assert result.success


@patch("dagster_notdiamond.resources.NotDiamondResource._wrap_with_usage_metadata")
@patch("dagster.AssetExecutionContext", autospec=AssetExecutionContext)
@patch("dagster_notdiamond.resources.NotDiamond")
def test_notdiamond_wrapper_with_graph_backed_asset(mock_client, mock_context, mock_wrapper):
    @op
    def model_version_op():
        return "openai/gpt-4o-mini"

    @op
    def training_file_op():
        return "some_training_file"

    @op
    def notdiamond_op(notdiamond_resource: NotDiamondResource, model_version, training_file):
        assert notdiamond_resource

        mock_completion = MagicMock()
        mock_usage = MagicMock()
        mock_usage.prompt_tokens = 1
        mock_usage.total_tokens = 1
        mock_usage.completion_tokens = 1
        mock_completion.usage = mock_usage
        mock_client.return_value.fine_tuning.jobs.create.return_value = mock_completion

        with notdiamond_resource.get_client(context=mock_context) as client:
            client.fine_tuning.jobs.create = with_usage_metadata(
                context=mock_context,
                output_name="notdiamond_asset",
                func=client.fine_tuning.jobs.create,
            )
            client.fine_tuning.jobs.create(model=model_version, training_file=training_file)

            mock_context.add_output_metadata.assert_called_with(
                metadata={
                    "notdiamond.calls": 1,
                    "notdiamond.total_tokens": 1,
                    "notdiamond.prompt_tokens": 1,
                    "notdiamond.completion_tokens": 1,
                },
                output_name="notdiamond_asset",
            )

    @graph_asset
    def notdiamond_asset():
        return notdiamond_op(model_version_op(), training_file_op())

    result = materialize_to_memory(
        [notdiamond_asset],
        resources={
            "notdiamond_resource": NotDiamondResource(api_key="xoxp-1234123412341234-12341234-1234")
        },
    )

    assert result.success


@patch("dagster_notdiamond.resources.NotDiamondResource._wrap_with_usage_metadata")
@patch("dagster.AssetExecutionContext", autospec=AssetExecutionContext)
@patch("dagster_notdiamond.resources.NotDiamond")
def test_notdiamond_wrapper_with_multi_asset(mock_client, mock_context, mock_wrapper):
    @multi_asset(
        specs=[AssetSpec("status"), AssetSpec("result")],
    )
    def notdiamond_multi_asset(notdiamond_resource: NotDiamondResource):
        assert notdiamond_resource

        mock_completion = MagicMock()
        mock_usage = MagicMock()
        mock_usage.prompt_tokens = 1
        mock_usage.total_tokens = 1
        mock_usage.completion_tokens = 1
        mock_completion.usage = mock_usage
        mock_client.return_value.fine_tuning.jobs.create.return_value = mock_completion

        with notdiamond_resource.get_client_for_asset(
            context=mock_context, asset_key=AssetKey("result")
        ) as client:
            client.fine_tuning.jobs.create = with_usage_metadata(
                context=mock_context,
                output_name="result",
                func=client.fine_tuning.jobs.create,
            )
            client.fine_tuning.jobs.create(
                model=["openai/gpt-4o-mini", "openai/gpt-4o"], training_file="some_training_file"
            )

            mock_context.add_output_metadata.assert_called_with(
                metadata={
                    "notdiamond.calls": 1,
                    "notdiamond.total_tokens": 1,
                    "notdiamond.prompt_tokens": 1,
                    "notdiamond.completion_tokens": 1,
                },
                output_name="result",
            )
        return None, None

    result = materialize_to_memory(
        [notdiamond_multi_asset],
        resources={
            "notdiamond_resource": NotDiamondResource(api_key="xoxp-1234123412341234-12341234-1234")
        },
    )

    assert result.success


@patch("dagster_notdiamond.resources.NotDiamondResource._wrap_with_usage_metadata")
@patch("dagster_notdiamond.resources.NotDiamond")
def test_notdiamond_wrapper_with_partitioned_asset(mock_client, mock_wrapper):
    notdiamond_partitions_def = StaticPartitionsDefinition([str(j) for j in range(5)])

    notdiamond_partitioned_assets = []

    for i in range(5):

        @asset(
            name=f"notdiamond_partitioned_asset_{i}",
            group_name="notdiamond_partitioned_assets",
            partitions_def=notdiamond_partitions_def,
        )
        def notdiamond_partitioned_asset(notdiamond_resource: NotDiamondResource):
            assert notdiamond_resource

            mock_context = MagicMock()
            mock_context.__class__ = AssetExecutionContext

            mock_completion = MagicMock()
            mock_usage = MagicMock()
            mock_usage.prompt_tokens = 1
            mock_usage.total_tokens = 1
            mock_usage.completion_tokens = 1
            mock_completion.usage = mock_usage
            mock_client.return_value.fine_tuning.jobs.create.return_value = mock_completion

            with notdiamond_resource.get_client(context=mock_context) as client:
                client.fine_tuning.jobs.create = with_usage_metadata(
                    context=mock_context,
                    output_name=None,
                    func=client.fine_tuning.jobs.create,
                )
                client.fine_tuning.jobs.create(
                    model=["openai/gpt-4o-mini", "openai/gpt-4o"],
                    training_file="some_training_file",
                )
                mock_context.add_output_metadata.assert_called_with(
                    {
                        "notdiamond.calls": 1,
                        "notdiamond.total_tokens": 1,
                        "notdiamond.prompt_tokens": 1,
                        "notdiamond.completion_tokens": 1,
                    },
                    None,
                )

        notdiamond_partitioned_assets.append(notdiamond_partitioned_asset)

    defs = Definitions(
        assets=notdiamond_partitioned_assets,
        jobs=[
            define_asset_job(
                name="notdiamond_partitioned_asset_job",
                selection=AssetSelection.groups("notdiamond_partitioned_assets"),
                partitions_def=notdiamond_partitions_def,
            )
        ],
        resources={
            "notdiamond_resource": NotDiamondResource(api_key="xoxp-1234123412341234-12341234-1234")
        },
    )

    for partition_key in notdiamond_partitions_def.get_partition_keys():
        result = defs.get_job_def("notdiamond_partitioned_asset_job").execute_in_process(
            partition_key=partition_key
        )
        assert result.success

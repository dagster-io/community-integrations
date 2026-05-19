from unittest.mock import MagicMock, patch

import pytest
from dagster import build_op_context
from datasets import Dataset

from dagster_hf_datasets.assets.dataset_asset import (
    hf_dataset_asset,
)


# ============================================================
# Fixtures
# ============================================================


@pytest.fixture
def mock_dataset():
    return Dataset.from_dict(
        {
            "text": [
                "hello",
                "world",
            ],
            "label": [0, 1],
        }
    )


@pytest.fixture
def mock_hf_resource(
    mock_dataset,
):
    resource = MagicMock()

    resource.load_dataset.return_value = mock_dataset

    return resource


# ============================================================
# Asset Construction
# ============================================================


def test_hf_dataset_asset_uses_function_name():
    @hf_dataset_asset(path="imdb")
    def my_asset():
        pass

    assert my_asset.op.name == "my_asset"


def test_hf_dataset_asset_uses_explicit_name():
    @hf_dataset_asset(
        path="imdb",
        name="custom_asset",
    )
    def my_asset():
        pass

    assert my_asset.op.name == "custom_asset"


# ============================================================
# Dataset Loading
# ============================================================


@patch("dagster_hf_datasets.assets.dataset_asset." "build_dataset_metadata")
def test_asset_loads_dataset(
    mock_build_metadata,
    mock_hf_resource,
):
    mock_build_metadata.return_value = {}

    @hf_dataset_asset(
        path="imdb",
        config="plain_text",
        split="train",
        revision="main",
        streaming=True,
    )
    def my_asset():
        pass

    context = build_op_context()

    result = my_asset(
        context,
        mock_hf_resource,
    )

    mock_hf_resource.load_dataset.assert_called_once_with(
        path="imdb",
        config="plain_text",
        split="train",
        revision="main",
        streaming=True,
    )

    assert result is not None


# ============================================================
# Metadata Propagation
# ============================================================


@patch("dagster_hf_datasets.assets.dataset_asset." "build_dataset_metadata")
def test_asset_output_metadata(
    mock_build_metadata,
    mock_hf_resource,
):
    mock_build_metadata.return_value = {
        "dataset_type": "Dataset",
        "num_rows": 2,
    }

    @hf_dataset_asset(
        path="imdb",
        config="plain_text",
        split="train",
    )
    def my_asset():
        pass

    context = build_op_context()

    result = my_asset(
        context,
        mock_hf_resource,
    )

    metadata = result.metadata

    assert metadata["path"] == "imdb"

    assert metadata["config"] == "plain_text"

    assert metadata["split"] == "train"

    assert metadata["dataset_type"] == "Dataset"

    assert metadata["num_rows"] == 2


# ============================================================
# Partition Resolution
# ============================================================


@patch("dagster_hf_datasets.assets.dataset_asset." "build_dataset_metadata")
def test_revision_partition_overrides_revision(
    mock_build_metadata,
    mock_hf_resource,
):
    mock_build_metadata.return_value = {}

    @hf_dataset_asset(
        path="imdb",
        revision="main",
    )
    def my_asset():
        pass

    context = build_op_context(partition_key=("revision:v2"))

    my_asset(
        context,
        mock_hf_resource,
    )

    mock_hf_resource.load_dataset.assert_called_once_with(
        path="imdb",
        config=None,
        split=None,
        revision="v2",
        streaming=False,
    )


@patch("dagster_hf_datasets.assets.dataset_asset." "build_dataset_metadata")
def test_config_partition_overrides_config(
    mock_build_metadata,
    mock_hf_resource,
):
    mock_build_metadata.return_value = {}

    @hf_dataset_asset(
        path="imdb",
        config="base",
    )
    def my_asset():
        pass

    context = build_op_context(partition_key=("config:qqp"))

    my_asset(
        context,
        mock_hf_resource,
    )

    mock_hf_resource.load_dataset.assert_called_once_with(
        path="imdb",
        config="qqp",
        split=None,
        revision=None,
        streaming=False,
    )


# ============================================================
# Partition Metadata
# ============================================================


@patch("dagster_hf_datasets.assets.dataset_asset." "build_dataset_metadata")
def test_partition_key_propagated_to_metadata(
    mock_build_metadata,
    mock_hf_resource,
):
    mock_build_metadata.return_value = {}

    @hf_dataset_asset(path="imdb")
    def my_asset():
        pass

    context = build_op_context(partition_key=("revision:main"))

    result = my_asset(
        context,
        mock_hf_resource,
    )

    assert result.metadata["partition_key"] == "revision:main"


# ============================================================
# Metadata Builder Integration
# ============================================================


@patch("dagster_hf_datasets.assets.dataset_asset." "build_dataset_metadata")
def test_build_dataset_metadata_called(
    mock_build_metadata,
    mock_hf_resource,
    mock_dataset,
):
    mock_build_metadata.return_value = {}

    @hf_dataset_asset(
        path="imdb",
        revision="main",
    )
    def my_asset():
        pass

    context = build_op_context()

    my_asset(
        context,
        mock_hf_resource,
    )

    mock_build_metadata.assert_called_once_with(
        mock_dataset,
        path="imdb",
        revision="main",
    )


# ============================================================
# Logging
# ============================================================


@patch("dagster_hf_datasets.assets.dataset_asset." "build_dataset_metadata")
def test_asset_logs_messages(
    mock_build_metadata,
    mock_hf_resource,
):
    mock_build_metadata.return_value = {
        "dataset_type": "Dataset",
        "streaming": False,
    }

    @hf_dataset_asset(path="imdb")
    def my_asset():
        pass

    context = build_op_context()

    context.log.info = MagicMock()

    my_asset(
        context,
        mock_hf_resource,
    )

    assert context.log.info.call_count >= 2


# ============================================================
# Output Value
# ============================================================


@patch("dagster_hf_datasets.assets.dataset_asset." "build_dataset_metadata")
def test_asset_returns_output_object(
    mock_build_metadata,
    mock_hf_resource,
    mock_dataset,
):
    mock_build_metadata.return_value = {}

    @hf_dataset_asset(path="imdb")
    def my_asset():
        pass

    context = build_op_context()

    result = my_asset(
        context,
        mock_hf_resource,
    )

    assert result.value == mock_dataset

from unittest.mock import MagicMock, patch

import pytest
from dagster import build_op_context
from datasets import (
    Dataset,
    DatasetDict,
)

from dagster_hf_datasets.assets.multi_asset import (
    _build_split_outputs,
    hf_multi_asset,
)


# ============================================================
# Fixtures
# ============================================================


@pytest.fixture
def dataset_dict():
    train = Dataset.from_dict(
        {
            "text": [
                "train1",
                "train2",
            ]
        }
    )

    test = Dataset.from_dict({"text": ["test1"]})

    return DatasetDict(
        {
            "train": train,
            "test": test,
        }
    )


@pytest.fixture
def mock_hf_resource(
    dataset_dict,
):
    resource = MagicMock()

    resource.load_dataset.return_value = dataset_dict

    return resource


# ============================================================
# Split Output Builder
# ============================================================


def test_build_split_outputs():
    outputs = _build_split_outputs(
        splits=[
            "train",
            "test",
        ],
        key_prefix="hf",
        io_manager_key="hf_io",
        metadata={"source": "hf"},
    )

    assert set(outputs.keys()) == {"train", "test"}

    train_output = outputs["train"]

    assert train_output.key_prefix == ["hf"]

    assert train_output.io_manager_key == "hf_io"


# ============================================================
# Split Resolution
# ============================================================


@patch("dagster_hf_datasets.assets.multi_asset." "get_dataset_split_names")
def test_hf_multi_asset_resolves_splits(
    mock_get_splits,
):
    mock_get_splits.return_value = [
        "train",
        "test",
    ]

    @hf_multi_asset(
        path="imdb",
    )
    def my_asset():
        pass

    assert my_asset is not None

    mock_get_splits.assert_called_once_with(
        path="imdb",
        config_name=None,
    )


@patch("dagster_hf_datasets.assets.multi_asset." "get_dataset_split_names")
def test_hf_multi_asset_requires_config_error(
    mock_get_splits,
):
    mock_get_splits.side_effect = ValueError("missing config")

    with pytest.raises(
        ValueError,
        match=("Failed to resolve dataset splits"),
    ):

        @hf_multi_asset(path="imdb")
        def my_asset():
            pass


# ============================================================
# Dataset Loading
# ============================================================


@patch("dagster_hf_datasets.assets.multi_asset." "get_dataset_split_names")
def test_multi_asset_loads_dataset(
    mock_get_splits,
    mock_hf_resource,
):
    mock_get_splits.return_value = [
        "train",
        "test",
    ]

    @hf_multi_asset(
        path="imdb",
        config="plain_text",
        revision="main",
    )
    def my_asset():
        pass

    context = build_op_context()

    list(
        my_asset(
            context,
            mock_hf_resource,
        )
    )

    mock_hf_resource.load_dataset.assert_called_once_with(
        path="imdb",
        config="plain_text",
        revision="main",
        streaming=False,
    )


# ============================================================
# Output Generation
# ============================================================


@patch("dagster_hf_datasets.assets.multi_asset." "build_dataset_metadata")
@patch("dagster_hf_datasets.assets.multi_asset." "get_dataset_split_names")
def test_multi_asset_yields_outputs(
    mock_get_splits,
    mock_build_metadata,
    dataset_dict,
    mock_hf_resource,
):
    mock_get_splits.return_value = [
        "train",
        "test",
    ]

    mock_build_metadata.return_value = {"num_rows": 2}

    @hf_multi_asset(path="imdb")
    def my_asset():
        pass

    context = build_op_context()

    outputs = list(
        my_asset(
            context,
            mock_hf_resource,
        )
    )

    assert len(outputs) == 2

    output_names = {output.output_name for output in outputs}

    assert output_names == {
        "train",
        "test",
    }


# ============================================================
# Selected Output Subsetting
# ============================================================


@patch("dagster_hf_datasets.assets.multi_asset." "build_dataset_metadata")
@patch("dagster_hf_datasets.assets.multi_asset." "get_dataset_split_names")
def test_multi_asset_respects_selected_outputs(
    mock_get_splits,
    mock_build_metadata,
    dataset_dict,
    mock_hf_resource,
):
    mock_get_splits.return_value = [
        "train",
        "test",
    ]

    mock_build_metadata.return_value = {}

    @hf_multi_asset(path="imdb")
    def my_asset():
        pass

    context = build_op_context(selected_output_names={"train"})

    outputs = list(
        my_asset(
            context,
            mock_hf_resource,
        )
    )

    assert len(outputs) == 1

    assert outputs[0].output_name == "train"


# ============================================================
# Partition Resolution
# ============================================================


@patch("dagster_hf_datasets.assets.multi_asset." "get_dataset_split_names")
def test_revision_partition_overrides_revision(
    mock_get_splits,
    mock_hf_resource,
):
    mock_get_splits.return_value = ["train"]

    @hf_multi_asset(
        path="imdb",
        revision="main",
    )
    def my_asset():
        pass

    context = build_op_context(partition_key=("revision:v2"))

    list(
        my_asset(
            context,
            mock_hf_resource,
        )
    )

    mock_hf_resource.load_dataset.assert_called_once_with(
        path="imdb",
        config=None,
        revision="v2",
        streaming=False,
    )


@patch("dagster_hf_datasets.assets.multi_asset." "get_dataset_split_names")
def test_config_partition_overrides_config(
    mock_get_splits,
    mock_hf_resource,
):
    mock_get_splits.return_value = ["train"]

    @hf_multi_asset(
        path="imdb",
        config="base",
    )
    def my_asset():
        pass

    context = build_op_context(partition_key=("config:qqp"))

    list(
        my_asset(
            context,
            mock_hf_resource,
        )
    )

    mock_hf_resource.load_dataset.assert_called_once_with(
        path="imdb",
        config="qqp",
        revision=None,
        streaming=False,
    )


# ============================================================
# Type Validation
# ============================================================


@patch("dagster_hf_datasets.assets.multi_asset." "get_dataset_split_names")
def test_multi_asset_requires_dataset_dict(
    mock_get_splits,
):
    mock_get_splits.return_value = ["train"]

    resource = MagicMock()

    resource.load_dataset.return_value = Dataset.from_dict({"x": [1]})

    @hf_multi_asset(path="imdb")
    def my_asset():
        pass

    context = build_op_context()

    with pytest.raises(
        TypeError,
        match=("hf_multi_asset expected " "DatasetDict"),
    ):
        list(
            my_asset(
                context,
                resource,
            )
        )


# ============================================================
# Metadata Propagation
# ============================================================


@patch("dagster_hf_datasets.assets.multi_asset." "build_dataset_metadata")
@patch("dagster_hf_datasets.assets.multi_asset." "get_dataset_split_names")
def test_output_metadata_contains_partition_key(
    mock_get_splits,
    mock_build_metadata,
    mock_hf_resource,
):
    mock_get_splits.return_value = ["train"]

    mock_build_metadata.return_value = {"num_rows": 2}

    @hf_multi_asset(path="imdb")
    def my_asset():
        pass

    context = build_op_context(partition_key=("revision:main"))

    outputs = list(
        my_asset(
            context,
            mock_hf_resource,
        )
    )

    metadata = outputs[0].metadata

    assert metadata["partition_key"] == "revision:main"

    assert metadata["path"] == "imdb"

    assert metadata["split"] == "train"

from unittest.mock import MagicMock, patch

import pytest
from datasets import (
    IterableDataset,
)

from dagster_hf_datasets._metadata._dataset_metadata import (
    build_dataset_metadata,
    extract_dataset_type,
    extract_execution_mode,
    extract_feature_names,
    extract_features,
    extract_fingerprint,
    extract_hub_metadata,
    extract_is_streaming,
    extract_num_rows,
    extract_num_shards,
    extract_revision,
)


# ============================================================
# Fixtures
# ============================================================


@pytest.fixture
def iterable_dataset():
    return IterableDataset.from_generator(
        lambda: iter(
            [
                {"text": "a"},
                {"text": "b"},
            ]
        )
    )


# ============================================================
# Dataset Type Extraction
# ============================================================


def test_extract_dataset_type_dataset(
    tiny_dataset,
):
    result = extract_dataset_type(tiny_dataset)

    assert result == "Dataset"


def test_extract_dataset_type_dataset_dict(
    tiny_dataset_dict,
):
    result = extract_dataset_type(tiny_dataset_dict)

    assert result == "DatasetDict"


# ============================================================
# Streaming Detection
# ============================================================


def test_extract_is_streaming_false(
    tiny_dataset,
):
    assert extract_is_streaming(tiny_dataset) is False


def test_extract_is_streaming_true(
    iterable_dataset,
):
    assert extract_is_streaming(iterable_dataset) is True


# ============================================================
# Execution Mode
# ============================================================


def test_extract_execution_mode_materialized(
    tiny_dataset,
):
    result = extract_execution_mode(tiny_dataset)

    assert result == "materialized"


def test_extract_execution_mode_streaming(
    iterable_dataset,
):
    result = extract_execution_mode(iterable_dataset)

    assert result == "lazy_streaming"


# ============================================================
# Row Count Extraction
# ============================================================


def test_extract_num_rows_dataset(
    tiny_dataset,
):
    result = extract_num_rows(tiny_dataset)

    assert result == 3


def test_extract_num_rows_dataset_dict(
    tiny_dataset_dict,
):
    result = extract_num_rows(tiny_dataset_dict)

    assert isinstance(result, dict)

    assert result == {
        "train": 2,
        "test": 1,
    }


def test_extract_num_rows_streaming_returns_none(
    iterable_dataset,
):
    result = extract_num_rows(iterable_dataset)

    assert result is None


# ============================================================
# Shard Extraction
# ============================================================


def test_extract_num_shards_dataset(
    tiny_dataset,
):
    result = extract_num_shards(tiny_dataset)

    assert result is None or isinstance(result, int)


def test_extract_num_shards_dataset_dict(
    tiny_dataset_dict,
):
    result = extract_num_shards(tiny_dataset_dict)

    assert isinstance(result, dict)


# ============================================================
# Feature Extraction
# ============================================================


def test_extract_features_dataset(
    tiny_dataset,
):
    result = extract_features(tiny_dataset)

    assert result == tiny_dataset.features


def test_extract_features_dataset_dict(
    tiny_dataset_dict,
):
    result = extract_features(tiny_dataset_dict)

    assert isinstance(result, dict)

    assert result["train"] == tiny_dataset_dict["train"].features


# ============================================================
# Feature Name Extraction
# ============================================================


def test_extract_feature_names_dataset(
    tiny_dataset,
):
    result = extract_feature_names(tiny_dataset)

    assert result == [
        "text",
        "label",
    ]


def test_extract_feature_names_dataset_dict(
    tiny_dataset_dict,
):
    result = extract_feature_names(tiny_dataset_dict)

    assert isinstance(result, dict)

    assert result == {
        "train": [
            "text",
            "label",
        ],
        "test": [
            "text",
            "label",
        ],
    }


# ============================================================
# Fingerprint Extraction
# ============================================================


def test_extract_fingerprint_dataset(
    tiny_dataset,
):
    result = extract_fingerprint(tiny_dataset)

    assert isinstance(
        result,
        str,
    )


def test_extract_fingerprint_dataset_dict(
    tiny_dataset_dict,
):
    result = extract_fingerprint(tiny_dataset_dict)

    assert isinstance(result, dict)

    assert "train" in result
    assert "test" in result


# ============================================================
# Revision Extraction
# ============================================================


def test_extract_revision_returns_none(
    tiny_dataset,
):
    result = extract_revision(tiny_dataset)

    assert result is None or isinstance(result, str)


# ============================================================
# Hub Metadata Extraction
# ============================================================


@patch("dagster_hf_datasets._metadata." "_dataset_metadata.api.dataset_info")
def test_extract_hub_metadata(
    mock_dataset_info,
):
    mock_info = MagicMock()

    mock_info.downloads = 1000
    mock_info.likes = 50
    mock_info.tags = ["nlp"]
    mock_info.private = False
    mock_info.gated = False
    mock_info.dataset_size = 1024
    mock_info.download_size = 512

    mock_dataset_info.return_value = mock_info

    metadata = extract_hub_metadata(
        path="imdb",
        revision="main",
    )

    assert metadata["hub_downloads"] == 1000

    assert metadata["hub_likes"] == 50

    assert metadata["hub_tags"] == ["nlp"]

    assert metadata["dataset_size_bytes"] == 1024

    assert metadata["download_size_bytes"] == 512


@patch("dagster_hf_datasets._metadata." "_dataset_metadata.api.dataset_info")
def test_extract_hub_metadata_failure_returns_empty(
    mock_dataset_info,
):
    mock_dataset_info.side_effect = RuntimeError("hub unavailable")

    metadata = extract_hub_metadata(path="imdb")

    assert metadata == {}


# ============================================================
# Metadata Builder
# ============================================================


@patch("dagster_hf_datasets._metadata." "_dataset_metadata.extract_hub_metadata")
def test_build_dataset_metadata(
    mock_hub_metadata,
    tiny_dataset,
):
    mock_hub_metadata.return_value = {"hub_downloads": 100}

    metadata = build_dataset_metadata(
        tiny_dataset,
        path="imdb",
        revision="main",
    )

    assert metadata["dataset_type"] == "Dataset"

    assert metadata["streaming"] is False

    assert metadata["execution_mode"] == "materialized"

    assert metadata["num_rows"] == 3

    assert metadata["features"] == [
        "text",
        "label",
    ]

    assert metadata["hub_downloads"] == 100


@patch("dagster_hf_datasets._metadata." "_dataset_metadata.extract_hub_metadata")
def test_build_dataset_metadata_without_path(
    mock_hub_metadata,
    tiny_dataset,
):
    metadata = build_dataset_metadata(tiny_dataset)

    mock_hub_metadata.assert_not_called()

    assert metadata["dataset_type"] == "Dataset"

    assert metadata["streaming"] is False

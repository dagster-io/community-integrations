from unittest.mock import patch

import pytest
from datasets import Dataset

from dagster_hf_datasets._export._publisher import (
    HFDatasetPublisher,
)


# ============================================================
# Fixtures
# ============================================================


@pytest.fixture
def tiny_dataset():
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
def publisher():
    return HFDatasetPublisher(
        repo_id="test-user/test-dataset",
        token="fake-token",
        private=True,
        exist_ok=True,
    )


# ============================================================
# Publish Flow
# ============================================================


@patch("datasets.Dataset.push_to_hub")
@patch(
    "dagster_hf_datasets._export._publisher." "HFDatasetPublisher._upload_dataset_card"
)
@patch("dagster_hf_datasets._export._publisher.create_repo")
def test_publish_creates_repo(
    mock_create_repo,
    mock_upload_card,
    mock_push_to_hub,
    publisher,
    tiny_dataset,
):
    publisher.publish(
        dataset=tiny_dataset,
        source_dataset="imdb",
    )

    mock_create_repo.assert_called_once_with(
        repo_id="test-user/test-dataset",
        repo_type="dataset",
        token="fake-token",
        private=True,
        exist_ok=True,
    )


@patch("datasets.Dataset.push_to_hub")
@patch(
    "dagster_hf_datasets._export._publisher." "HFDatasetPublisher._upload_dataset_card"
)
@patch("dagster_hf_datasets._export._publisher.create_repo")
def test_publish_pushes_dataset(
    mock_create_repo,
    mock_upload_card,
    mock_push_to_hub,
    publisher,
    tiny_dataset,
):
    publisher.publish(
        dataset=tiny_dataset,
        source_dataset="imdb",
    )

    mock_push_to_hub.assert_called_once_with(
        repo_id="test-user/test-dataset",
        token="fake-token",
        commit_message=("Upload dataset via " "dagster-hf-datasets"),
    )


@patch("datasets.Dataset.push_to_hub")
@patch(
    "dagster_hf_datasets._export._publisher." "HFDatasetPublisher._upload_dataset_card"
)
@patch("dagster_hf_datasets._export._publisher.create_repo")
def test_publish_returns_dataset_url(
    mock_create_repo,
    mock_upload_card,
    mock_push_to_hub,
    publisher,
    tiny_dataset,
):
    result = publisher.publish(
        dataset=tiny_dataset,
        source_dataset="imdb",
    )

    assert result == "https://huggingface.co/datasets/" "test-user/test-dataset"


# ============================================================
# Dataset Card Upload
# ============================================================


@patch("datasets.Dataset.push_to_hub")
@patch("dagster_hf_datasets._export._publisher.upload_file")
@patch("dagster_hf_datasets._export._publisher.create_repo")
def test_publish_uploads_dataset_card(
    mock_create_repo,
    mock_upload_file,
    mock_push_to_hub,
    publisher,
    tiny_dataset,
):
    publisher.publish(
        dataset=tiny_dataset,
        source_dataset="imdb",
        create_dataset_card=True,
    )

    mock_upload_file.assert_called_once()

    kwargs = mock_upload_file.call_args.kwargs

    assert kwargs["path_in_repo"] == "README.md"

    assert kwargs["repo_id"] == "test-user/test-dataset"

    assert kwargs["repo_type"] == "dataset"


@patch("datasets.Dataset.push_to_hub")
@patch("dagster_hf_datasets._export._publisher.upload_file")
@patch("dagster_hf_datasets._export._publisher.create_repo")
def test_publish_skips_dataset_card_when_disabled(
    mock_create_repo,
    mock_upload_file,
    mock_push_to_hub,
    publisher,
    tiny_dataset,
):
    publisher.publish(
        dataset=tiny_dataset,
        source_dataset="imdb",
        create_dataset_card=False,
    )

    mock_upload_file.assert_not_called()


# ============================================================
# Dataset Card Commit Message
# ============================================================


@patch("datasets.Dataset.push_to_hub")
@patch("dagster_hf_datasets._export._publisher.upload_file")
@patch("dagster_hf_datasets._export._publisher.create_repo")
def test_dataset_card_commit_message(
    mock_create_repo,
    mock_upload_file,
    mock_push_to_hub,
    publisher,
    tiny_dataset,
):
    publisher.publish(
        dataset=tiny_dataset,
        source_dataset="imdb",
    )

    kwargs = mock_upload_file.call_args.kwargs

    assert kwargs["commit_message"] == (
        "Upload dataset via " "dagster-hf-datasets " "(dataset card)"
    )


# ============================================================
# Commit Message Handling
# ============================================================


@patch("datasets.Dataset.push_to_hub")
@patch(
    "dagster_hf_datasets._export._publisher." "HFDatasetPublisher._upload_dataset_card"
)
@patch("dagster_hf_datasets._export._publisher.create_repo")
def test_custom_commit_message_used(
    mock_create_repo,
    mock_upload_card,
    mock_push_to_hub,
    publisher,
    tiny_dataset,
):
    publisher.publish(
        dataset=tiny_dataset,
        source_dataset="imdb",
        commit_message=("custom upload message"),
    )

    mock_push_to_hub.assert_called_once_with(
        repo_id="test-user/test-dataset",
        token="fake-token",
        commit_message=("custom upload message"),
    )


# ============================================================
# Failure Propagation
# ============================================================


@patch(
    "datasets.Dataset.push_to_hub",
    side_effect=RuntimeError("push failed"),
)
@patch(
    "dagster_hf_datasets._export._publisher." "HFDatasetPublisher._upload_dataset_card"
)
@patch("dagster_hf_datasets._export._publisher.create_repo")
def test_publish_propagates_push_failure(
    mock_create_repo,
    mock_upload_card,
    mock_push_to_hub,
    publisher,
    tiny_dataset,
):
    with pytest.raises(
        RuntimeError,
        match="push failed",
    ):
        publisher.publish(
            dataset=tiny_dataset,
            source_dataset="imdb",
        )


@patch("dagster_hf_datasets._export._publisher.create_repo")
def test_publish_propagates_repo_creation_failure(
    mock_create_repo,
    publisher,
    tiny_dataset,
):
    mock_create_repo.side_effect = RuntimeError("repo creation failed")

    with pytest.raises(
        RuntimeError,
        match="repo creation failed",
    ):
        publisher.publish(
            dataset=tiny_dataset,
            source_dataset="imdb",
        )


# ============================================================
# Repository Configuration
# ============================================================


@patch("datasets.Dataset.push_to_hub")
@patch(
    "dagster_hf_datasets._export._publisher." "HFDatasetPublisher._upload_dataset_card"
)
@patch("dagster_hf_datasets._export._publisher.create_repo")
def test_publish_passes_repo_configuration(
    mock_create_repo,
    mock_upload_card,
    mock_push_to_hub,
    tiny_dataset,
):
    publisher = HFDatasetPublisher(
        repo_id="org/dataset",
        token="abc",
        private=False,
        exist_ok=False,
    )

    publisher.publish(
        dataset=tiny_dataset,
        source_dataset="imdb",
    )

    mock_create_repo.assert_called_once_with(
        repo_id="org/dataset",
        repo_type="dataset",
        token="abc",
        private=False,
        exist_ok=False,
    )

import os
from unittest.mock import patch

import pytest
from datasets import (
    Dataset,
    DatasetDict,
)

from dagster_hf_datasets.resources.huggingface_resource import (
    HuggingFaceResource,
)


# ============================================================
# Fixtures
# ============================================================


@pytest.fixture
def resource():
    return HuggingFaceResource()


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
def tiny_dataset_dict():
    train = Dataset.from_dict(
        {
            "text": [
                "train1",
                "train2",
            ],
            "label": [0, 1],
        }
    )

    test = Dataset.from_dict(
        {
            "text": ["test1"],
            "label": [1],
        }
    )

    return DatasetDict(
        {
            "train": train,
            "test": test,
        }
    )


# ============================================================
# Token Resolution
# ============================================================


def test_resolve_token_from_explicit_token():
    resource = HuggingFaceResource(token="explicit-token")

    assert resource._resolve_token() == "explicit-token"


def test_resolve_token_from_file(
    temp_dir,
):
    token_file = temp_dir / "token.txt"

    token_file.write_text(
        "file-token",
        encoding="utf-8",
    )

    resource = HuggingFaceResource(token_path=str(token_file))

    assert resource._resolve_token() == "file-token"


def test_resolve_token_from_environment(
    monkeypatch,
):
    monkeypatch.setenv(
        "HF_TOKEN",
        "env-token",
    )

    resource = HuggingFaceResource()

    assert resource._resolve_token() == "env-token"


def test_explicit_token_takes_precedence(
    monkeypatch,
    temp_dir,
):
    token_file = temp_dir / "token.txt"

    token_file.write_text(
        "file-token",
        encoding="utf-8",
    )

    monkeypatch.setenv(
        "HF_TOKEN",
        "env-token",
    )

    resource = HuggingFaceResource(
        token="explicit-token",
        token_path=str(token_file),
    )

    assert resource._resolve_token() == "explicit-token"


def test_missing_token_returns_none():
    resource = HuggingFaceResource()

    assert resource._resolve_token() is None


# ============================================================
# Offline Mode
# ============================================================


def test_configure_offline_mode_enabled(
    monkeypatch,
):
    monkeypatch.delenv(
        "HF_HUB_OFFLINE",
        raising=False,
    )

    resource = HuggingFaceResource(offline=True)

    resource._configure_offline_mode()

    assert os.environ["HF_HUB_OFFLINE"] == "1"


def test_configure_offline_mode_disabled(
    monkeypatch,
):
    monkeypatch.setenv(
        "HF_HUB_OFFLINE",
        "1",
    )

    resource = HuggingFaceResource(offline=False)

    resource._configure_offline_mode()

    assert "HF_HUB_OFFLINE" not in os.environ


# ============================================================
# Dataset Loading
# ============================================================


@patch("dagster_hf_datasets.resources." "huggingface_resource.load_dataset")
def test_load_dataset_forwards_arguments(
    mock_load_dataset,
):
    resource = HuggingFaceResource(
        token="abc",
        cache_dir="/tmp/cache",
    )

    resource.load_dataset(
        path="imdb",
        config="plain_text",
        split="train",
        revision="main",
        streaming=True,
    )

    mock_load_dataset.assert_called_once_with(
        path="imdb",
        name="plain_text",
        split="train",
        revision="main",
        streaming=True,
        token="abc",
        cache_dir="/tmp/cache",
    )


@patch("dagster_hf_datasets.resources." "huggingface_resource.load_dataset")
def test_load_dataset_forwards_kwargs(
    mock_load_dataset,
):
    resource = HuggingFaceResource()

    resource.load_dataset(
        path="imdb",
        trust_remote_code=True,
    )

    mock_load_dataset.assert_called_once()

    kwargs = mock_load_dataset.call_args.kwargs

    assert kwargs["trust_remote_code"] is True


# ============================================================
# Row Count Extraction
# ============================================================


def test_get_num_rows_dataset(
    tiny_dataset,
):
    result = HuggingFaceResource.get_num_rows(tiny_dataset)

    assert result == 2


def test_get_num_rows_dataset_dict(
    tiny_dataset_dict,
):
    result = HuggingFaceResource.get_num_rows(tiny_dataset_dict)

    assert result == {
        "train": 2,
        "test": 1,
    }


# ============================================================
# Feature Extraction
# ============================================================


def test_get_features_dataset(
    tiny_dataset,
):
    result = HuggingFaceResource.get_features(tiny_dataset)

    assert result == (tiny_dataset.features)


def test_get_features_dataset_dict(
    tiny_dataset_dict,
):
    result = HuggingFaceResource.get_features(tiny_dataset_dict)

    assert result["train"] == tiny_dataset_dict["train"].features


# ============================================================
# Fingerprint Extraction
# ============================================================


def test_get_fingerprint_dataset(
    tiny_dataset,
):
    result = HuggingFaceResource.get_fingerprint(tiny_dataset)

    assert isinstance(
        result,
        str,
    )


def test_get_fingerprint_dataset_dict(
    tiny_dataset_dict,
):
    result = HuggingFaceResource.get_fingerprint(tiny_dataset_dict)

    assert "train" in result
    assert "test" in result


# ============================================================
# Revision Extraction
# ============================================================


def test_get_revision_returns_none_without_version(
    tiny_dataset,
):
    result = HuggingFaceResource.get_revision(tiny_dataset)

    assert result is None or isinstance(result, str)


# ============================================================
# setup_for_execution
# ============================================================


def test_setup_for_execution_calls_offline_configuration():
    resource = HuggingFaceResource(offline=True)

    with patch.object(
        HuggingFaceResource,
        "_configure_offline_mode",
    ) as mock_configure:
        resource.setup_for_execution(None)

    mock_configure.assert_called_once()

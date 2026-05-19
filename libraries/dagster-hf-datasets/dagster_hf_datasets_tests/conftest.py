from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Generator

import pytest
from datasets import Dataset, DatasetDict
from dagster import build_init_resource_context


# -------------------------------------------------------------------
# Temporary directories
# -------------------------------------------------------------------


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


# -------------------------------------------------------------------
# Tiny HF datasets
# -------------------------------------------------------------------


@pytest.fixture
def tiny_dataset() -> Dataset:
    return Dataset.from_dict(
        {
            "text": [
                "hello",
                "world",
                "dagster",
            ],
            "label": [
                0,
                1,
                0,
            ],
        }
    )


@pytest.fixture
def tiny_dataset_with_metadata() -> Dataset:
    ds = Dataset.from_dict(
        {
            "text": ["a", "b"],
            "score": [0.1, 0.9],
        }
    )

    ds.info.description = "Tiny test dataset"

    return ds


@pytest.fixture
def tiny_dataset_dict() -> DatasetDict:
    train = Dataset.from_dict(
        {
            "text": ["train1", "train2"],
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


# -------------------------------------------------------------------
# Empty / edge-case datasets
# -------------------------------------------------------------------


@pytest.fixture
def empty_dataset() -> Dataset:
    return Dataset.from_dict(
        {
            "text": [],
            "label": [],
        }
    )


@pytest.fixture
def nested_dataset() -> Dataset:
    return Dataset.from_dict(
        {
            "id": [1, 2],
            "meta": [
                {"source": "a"},
                {"source": "b"},
            ],
        }
    )


# -------------------------------------------------------------------
# Mock HF resource config
# -------------------------------------------------------------------


@pytest.fixture
def hf_resource_config() -> dict:
    return {
        "token": "fake-token",
        "repo_id": "test-user/test-dataset",
    }


# -------------------------------------------------------------------
# Dagster resource context
# -------------------------------------------------------------------


@pytest.fixture
def dagster_resource_context():
    return build_init_resource_context()


# -------------------------------------------------------------------
# Common metadata fixture
# -------------------------------------------------------------------


@pytest.fixture
def sample_metadata() -> dict:
    return {
        "description": "Test dataset",
        "license": "apache-2.0",
        "tags": ["test", "unit"],
        "task_categories": ["text-classification"],
    }

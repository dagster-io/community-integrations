from __future__ import annotations

from typing import Any

from datasets import (
    Dataset,
    DatasetDict,
    Features,
    IterableDataset,
    IterableDatasetDict,
)

type HFDatasetLike = Dataset | DatasetDict | IterableDataset | IterableDatasetDict


def extract_num_rows(
    dataset: HFDatasetLike,
) -> int | dict[str, int] | None:
    """
    Extract row count metadata from a Hugging Face dataset object.

    Streaming datasets may not expose row counts.

    Args:
        dataset:
            Hugging Face dataset object.

    Returns:
        Integer row count, split mapping, or None.
    """
    if isinstance(dataset, Dataset):
        return dataset.num_rows

    if isinstance(dataset, DatasetDict):
        return {split: ds.num_rows for split, ds in dataset.items()}

    return None


def extract_features(
    dataset: HFDatasetLike,
) -> Features | dict[str, Features] | None:
    """
    Extract dataset feature/schema metadata.

    Args:
        dataset:
            Hugging Face dataset object.

    Returns:
        Features object, split mapping, or None.
    """
    if isinstance(dataset, (Dataset, IterableDataset)):
        return dataset.features

    if isinstance(dataset, (DatasetDict, IterableDatasetDict)):
        return {split: ds.features for split, ds in dataset.items()}

    return None


def extract_feature_names(
    dataset: HFDatasetLike,
) -> list[str] | dict[str, list[str]] | None:
    """
    Extract dataset feature/column names.

    Args:
        dataset:
            Hugging Face dataset object.

    Returns:
        List of feature names, split mapping, or None.
    """
    features = extract_features(dataset)

    if features is None:
        return None

    if isinstance(features, Features):
        return list(features.keys())

    return {
        split: list(split_features.keys()) for split, split_features in features.items()
    }


def extract_fingerprint(
    dataset: HFDatasetLike,
) -> str | dict[str, str] | None:
    """
    Extract Hugging Face dataset fingerprint metadata.

    Fingerprints are useful for:
    - reproducibility
    - caching
    - lineage tracking

    Args:
        dataset:
            Hugging Face dataset object.

    Returns:
        Fingerprint string, split mapping, or None.
    """
    if isinstance(dataset, Dataset):
        return dataset._fingerprint

    if isinstance(dataset, DatasetDict):
        return {split: ds._fingerprint for split, ds in dataset.items()}

    return None


def extract_revision(
    dataset: HFDatasetLike,
) -> str | None:
    """
    Attempt to extract dataset revision/version metadata.

    Args:
        dataset:
            Hugging Face dataset object.

    Returns:
        Revision/version string if available.
    """
    try:
        return dataset.info.version.version
    except AttributeError:
        return None


def build_dataset_metadata(
    dataset: HFDatasetLike,
) -> dict[str, Any]:
    """
    Build normalized metadata dictionary suitable for Dagster
    asset materialization metadata.

    Args:
        dataset:
            Hugging Face dataset object.

    Returns:
        Normalized metadata dictionary.
    """
    return {
        "num_rows": extract_num_rows(dataset),
        "features": extract_feature_names(dataset),
        "fingerprint": extract_fingerprint(dataset),
        "revision": extract_revision(dataset),
    }

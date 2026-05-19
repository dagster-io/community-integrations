from __future__ import annotations

from typing import Any

from datasets import (
    Dataset,
    DatasetDict,
    Features,
    IterableDataset,
    IterableDatasetDict,
)
from huggingface_hub import (
    HfApi,
)

type HFDatasetLike = Dataset | DatasetDict | IterableDataset | IterableDatasetDict

api = HfApi()


def extract_dataset_type(
    dataset: HFDatasetLike,
) -> str:
    """
    Extract runtime dataset type.
    """
    return type(dataset).__name__


def extract_is_streaming(
    dataset: HFDatasetLike,
) -> bool:
    """
    Determine whether the dataset
    uses streaming semantics.
    """
    return isinstance(
        dataset,
        (
            IterableDataset,
            IterableDatasetDict,
        ),
    )


def extract_execution_mode(
    dataset: HFDatasetLike,
) -> str:
    """
    Extract orchestration execution mode.
    """
    if extract_is_streaming(dataset):
        return "lazy_streaming"

    return "materialized"


def extract_num_rows(
    dataset: HFDatasetLike,
) -> int | dict[str, int] | None:
    """
    Extract row count metadata from a
    Hugging Face dataset object.

    Streaming datasets may not expose
    row counts.

    Args:
        dataset:
            Hugging Face dataset object.

    Returns:
        Integer row count, split mapping,
        or None.
    """
    if isinstance(dataset, Dataset):
        return dataset.num_rows

    if isinstance(dataset, DatasetDict):
        return {split: ds.num_rows for split, ds in dataset.items()}

    return None


def extract_num_shards(
    dataset: HFDatasetLike,
) -> int | dict[str, int] | None:
    """
    Extract dataset shard metadata.
    """

    if isinstance(
        dataset,
        (
            Dataset,
            IterableDataset,
        ),
    ):
        return getattr(
            dataset,
            "num_shards",
            None,
        )

    if isinstance(
        dataset,
        (
            DatasetDict,
            IterableDatasetDict,
        ),
    ):
        return {
            split: getattr(
                ds,
                "num_shards",
                None,
            )
            for split, ds in dataset.items()
        }

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
        Features object, split mapping,
        or None.
    """
    if isinstance(
        dataset,
        (
            Dataset,
            IterableDataset,
        ),
    ):
        return dataset.features

    if isinstance(
        dataset,
        (
            DatasetDict,
            IterableDatasetDict,
        ),
    ):
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
        List of feature names,
        split mapping, or None.
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
    Extract Hugging Face dataset
    fingerprint metadata.

    Fingerprints are useful for:
    - reproducibility
    - caching
    - lineage tracking

    Args:
        dataset:
            Hugging Face dataset object.

    Returns:
        Fingerprint string,
        split mapping, or None.
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
    Attempt to extract dataset
    revision/version metadata.

    Args:
        dataset:
            Hugging Face dataset object.

    Returns:
        Revision/version string
        if available.
    """
    try:
        return dataset.info.version.version

    except AttributeError:
        return None


def extract_hub_metadata(
    *,
    path: str,
    revision: str | None = None,
) -> dict[str, Any]:
    """
    Extract Hugging Face Hub dataset metadata.

    Best-effort enrichment only.

    Args:
        path:
            Hugging Face dataset repository ID.

        revision:
            Optional dataset revision.

    Returns:
        Metadata dictionary.
    """

    try:
        info = api.dataset_info(
            repo_id=path,
            revision=revision,
        )

    except Exception:
        return {}

    metadata: dict[str, Any] = {}

    metadata["hub_downloads"] = info.downloads

    metadata["hub_likes"] = info.likes

    metadata["hub_tags"] = info.tags

    metadata["hub_private"] = info.private

    metadata["hub_gated"] = info.gated

    if (
        getattr(
            info,
            "dataset_size",
            None,
        )
        is not None
    ):
        metadata["dataset_size_bytes"] = info.dataset_size

    if (
        getattr(
            info,
            "download_size",
            None,
        )
        is not None
    ):
        metadata["download_size_bytes"] = info.download_size

    return metadata


def build_dataset_metadata(
    dataset: HFDatasetLike,
    *,
    path: str | None = None,
    revision: str | None = None,
) -> dict[str, Any]:
    """
    Build normalized metadata dictionary
    suitable for Dagster asset
    materialization metadata.

    Metadata sources:
    - runtime dataset inspection
    - streaming semantics
    - Hugging Face Hub enrichment

    Args:
        dataset:
            Hugging Face dataset object.

        path:
            Optional Hugging Face Hub
            repository path.

        revision:
            Optional dataset revision.

    Returns:
        Normalized metadata dictionary.
    """

    metadata = {
        "dataset_type": (extract_dataset_type(dataset)),
        "streaming": (extract_is_streaming(dataset)),
        "execution_mode": (extract_execution_mode(dataset)),
        "num_rows": (extract_num_rows(dataset)),
        "num_shards": (extract_num_shards(dataset)),
        "features": (extract_feature_names(dataset)),
        "fingerprint": (extract_fingerprint(dataset)),
        "revision": (extract_revision(dataset)),
    }

    if path is not None:
        metadata.update(
            extract_hub_metadata(
                path=path,
                revision=revision,
            )
        )

    return metadata

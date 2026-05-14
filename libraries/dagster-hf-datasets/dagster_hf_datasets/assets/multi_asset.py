from __future__ import annotations

from collections.abc import Callable
from typing import Any

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    AssetOut,
    multi_asset,
)

from datasets import (
    DatasetDict,
    IterableDatasetDict,
)

from dagster_hf_datasets._metadata import build_dataset_metadata
from dagster_hf_datasets.resources import HuggingFaceResource


def hf_multi_asset(
    *,
    path: str,
    config: str | None = None,
    revision: str | None = None,
    streaming: bool = False,
    group_name: str | None = None,
    key_prefix: str | list[str] | None = None,
    metadata: dict[str, Any] | None = None,
    tags: dict[str, str] | None = None,
) -> Callable[[Callable[..., Any]], Any]:
    """
    Dagster multi-asset decorator for Hugging Face DatasetDict assets.

    Automatically expands DatasetDict splits into Dagster assets.

    Example:
        @hf_multi_asset(
            path="glue",
            config="qqp",
        )
        def glue_assets():
            ...

    Produces:
        - train
        - validation
        - test

    Args:
        path:
            Hugging Face dataset repository path.
        config:
            Dataset configuration name.
        revision:
            Dataset revision/tag/commit hash.
        streaming:
            Enable streaming mode.
        group_name:
            Dagster asset group.
        key_prefix:
            Dagster asset key prefix.
        metadata:
            Additional Dagster metadata.
        tags:
            Dagster asset tags.
    """

    def decorator(fn: Callable[..., Any]) -> Any:
        split_outputs = {
            "train": AssetOut(is_required=False),
            "validation": AssetOut(is_required=False),
            "test": AssetOut(is_required=False),
        }

        @multi_asset(
            outs=split_outputs,
            group_name=group_name,
            key_prefix=key_prefix,
            metadata=metadata,
            tags=tags,
            can_subset=True,
        )
        def _multi_asset(
            context: AssetExecutionContext,
            huggingface: HuggingFaceResource,
        ) -> dict[str, MaterializeResult]:
            dataset = huggingface.load_dataset(
                path=path,
                config=config,
                revision=revision,
                streaming=streaming,
            )

            if not isinstance(
                dataset,
                (DatasetDict, IterableDatasetDict),
            ):
                raise TypeError(
                    "hf_multi_asset requires a DatasetDict "
                    "or IterableDatasetDict."
                )

            results: dict[str, MaterializeResult] = {}

            for split_name, split_dataset in dataset.items():
                if (
                    context.selected_output_names
                    and split_name
                    not in context.selected_output_names
                ):
                    continue

                split_metadata = build_dataset_metadata(
                    split_dataset
                )

                results[split_name] = MaterializeResult(
                    metadata={
                        "path": path,
                        "config": config,
                        "split": split_name,
                        "streaming": streaming,
                        **split_metadata,
                    },
                    value=split_dataset,
                )

            context.log.info(
                f"Loaded Hugging Face DatasetDict: {path}"
            )

            return results

        return _multi_asset

    return decorator
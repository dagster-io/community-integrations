from __future__ import annotations

from collections.abc import Callable
from typing import Any

from dagster import (
    AssetExecutionContext,
    AssetOut,
    MaterializeResult,
    PartitionsDefinition,
    multi_asset,
)

from datasets import (
    DatasetDict,
    IterableDatasetDict,
)

from dagster_hf_datasets._metadata import (
    build_dataset_metadata,
)
from dagster_hf_datasets._partitions import (
    HFPartitionMapping,
)
from dagster_hf_datasets.resources import (
    HuggingFaceResource,
)


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
    io_manager_key: str | None = None,
    partitions_def: PartitionsDefinition | None = None,
) -> Callable[[Callable[..., Any]], Any]:
    """
    Dagster multi-asset decorator for Hugging Face
    DatasetDict assets.

    Automatically expands DatasetDict splits into
    Dagster assets.

    Supported orchestration semantics:
    - metadata propagation
    - partition-aware loading
    - IO manager integration
    - selective materialization
    """

    def decorator(fn: Callable[..., Any]) -> Any:
        split_outputs = {
            "train": AssetOut(
                is_required=False,
                io_manager_key=io_manager_key,
            ),
            "validation": AssetOut(
                is_required=False,
                io_manager_key=io_manager_key,
            ),
            "test": AssetOut(
                is_required=False,
                io_manager_key=io_manager_key,
            ),
        }

        @multi_asset(
            outs=split_outputs,
            group_name=group_name,
            key_prefix=key_prefix,
            metadata=metadata,
            tags=tags,
            can_subset=True,
            partitions_def=partitions_def,
        )
        def _multi_asset(
            context: AssetExecutionContext,
            huggingface: HuggingFaceResource,
        ) -> dict[str, MaterializeResult]:
            resolved_revision = revision
            resolved_config = config

            if context.has_partition_key:
                partition = (
                    HFPartitionMapping.from_partition_key(
                        context.partition_key
                    )
                )

                if partition.is_revision:
                    resolved_revision = partition.value

                elif partition.is_config:
                    resolved_config = partition.value

            dataset = huggingface.load_dataset(
                path=path,
                config=resolved_config,
                revision=resolved_revision,
                streaming=streaming,
            )

            if not isinstance(
                dataset,
                (
                    DatasetDict,
                    IterableDatasetDict,
                ),
            ):
                raise TypeError(
                    "hf_multi_asset requires a "
                    "DatasetDict or "
                    "IterableDatasetDict."
                )

            results: dict[str, MaterializeResult] = {}

            for split_name, split_dataset in (
                dataset.items()
            ):
                if (
                    context.selected_output_names
                    and split_name
                    not in context.selected_output_names
                ):
                    continue

                split_metadata = (
                    build_dataset_metadata(
                        split_dataset
                    )
                )

                results[split_name] = (
                    MaterializeResult(
                        metadata={
                            "path": path,
                            "config": resolved_config,
                            "split": split_name,
                            "revision": (
                                resolved_revision
                            ),
                            "streaming": streaming,
                            "partition_key": (
                                context.partition_key
                                if context.has_partition_key
                                else None
                            ),
                            **split_metadata,
                        },
                        value=split_dataset,
                    )
                )

            context.log.info(
                f"Loaded Hugging Face "
                f"DatasetDict: {path}"
            )

            return results

        return _multi_asset

    return decorator
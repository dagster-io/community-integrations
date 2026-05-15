from __future__ import annotations

from collections.abc import Callable
from typing import Any

from dagster import (
    AssetOut,
    Output,
    PartitionsDefinition,
    multi_asset,
)

from datasets import (
    DatasetDict,
    IterableDatasetDict,
    get_dataset_split_names,
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


def _build_split_outputs(
    *,
    splits: list[str],
    key_prefix: str | list[str] | None,
    io_manager_key: str | None,
    metadata: dict[str, Any] | None,
) -> dict[str, AssetOut]:
    """
    Build Dagster AssetOut definitions
    for each Hugging Face dataset split.
    """

    return {
        split: AssetOut(
            key_prefix=key_prefix,
            metadata=metadata,
            is_required=False,
            io_manager_key=io_manager_key,
        )
        for split in splits
    }


def hf_multi_asset(
    *,
    path: str,
    config: str | None = None,
    revision: str | None = None,
    streaming: bool = False,
    group_name: str | None = None,
    key_prefix: str | list[str] | None = None,
    metadata: dict[str, Any] | None = None,
    op_tags: dict[str, str] | None = None,
    io_manager_key: str | None = None,
    partitions_def: PartitionsDefinition | None = None,
) -> Callable[[Callable[..., Any]], Any]:
    """
    Dagster multi-asset decorator for Hugging Face
    DatasetDict assets.

    Automatically expands Hugging Face dataset
    splits into Dagster assets.

    Supported orchestration semantics:
    - metadata propagation
    - partition-aware loading
    - IO manager integration
    - selective materialization
    - streaming datasets
    """

    try:
        splits = list(
            get_dataset_split_names(
                path=path,
                config_name=config,
            )
        )

    except ValueError as exc:
        raise ValueError(
            f"Failed to resolve dataset splits "
            f"for '{path}'. "
            f"This dataset likely requires an "
            f"explicit `config=` argument."
        ) from exc

    split_outputs = _build_split_outputs(
        splits=splits,
        key_prefix=key_prefix,
        io_manager_key=io_manager_key,
        metadata=metadata,
    )

    def decorator(fn: Callable[..., Any]) -> Any:
        compute_name = fn.__name__

        @multi_asset(
            name=compute_name,
            outs=split_outputs,
            group_name=group_name,
            op_tags=op_tags,
            can_subset=True,
            partitions_def=partitions_def,
        )
        def _multi_asset(
            context,
            huggingface: HuggingFaceResource,
        ):
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

            for split_name, split_dataset in dataset.items():
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

                yield Output(
                    value=split_dataset,
                    output_name=split_name,
                    metadata={
                        "path": path,
                        "config": resolved_config,
                        "split": split_name,
                        "revision": resolved_revision,
                        "streaming": streaming,
                        "partition_key": (
                            context.partition_key
                            if context.has_partition_key
                            else None
                        ),
                        **split_metadata,
                    },
                )

            context.log.info(
                f"Loaded Hugging Face "
                f"DatasetDict: {path}"
            )

        return _multi_asset

    return decorator
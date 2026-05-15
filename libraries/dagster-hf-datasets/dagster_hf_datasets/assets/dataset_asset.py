from __future__ import annotations

from collections.abc import Callable
from typing import Any

from dagster import (
    Output,
    PartitionsDefinition,
    asset,
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


def hf_dataset_asset(
    *,
    path: str,
    config: str | None = None,
    split: str | None = None,
    revision: str | None = None,
    streaming: bool = False,
    name: str | None = None,
    group_name: str | None = None,
    key_prefix: str | list[str] | None = None,
    metadata: dict[str, Any] | None = None,
    tags: dict[str, str] | None = None,
    io_manager_key: str | None = None,
    partitions_def: PartitionsDefinition | None = None,
) -> Callable[[Callable[..., Any]], Any]:
    """
    Dagster asset decorator for Hugging Face datasets.

    This decorator preserves Hugging Face dataset
    loading semantics while exposing datasets as
    first-class Dagster assets.

    Supported orchestration semantics:
    - metadata propagation
    - partition-aware loading
    - IO manager integration
    - streaming datasets
    """

    def decorator(
        fn: Callable[..., Any],
    ) -> Any:
        """
        Preserve the decorated function name
        as the Dagster asset key by default.

        This keeps Dagster dependency resolution
        intuitive and predictable.
        """

        asset_name = name or fn.__name__

        @asset(
            name=asset_name,
            group_name=group_name,
            key_prefix=key_prefix,
            metadata=metadata,
            tags=tags,
            io_manager_key=io_manager_key,
            partitions_def=partitions_def,
        )
        def _asset(
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
                    resolved_revision = (
                        partition.value
                    )

                elif partition.is_config:
                    resolved_config = (
                        partition.value
                    )

            dataset = huggingface.load_dataset(
                path=path,
                config=resolved_config,
                split=split,
                revision=resolved_revision,
                streaming=streaming,
            )

            dataset_metadata = (
                build_dataset_metadata(
                    dataset,
                    path=path,
                    revision=resolved_revision,
                )
            )

            context.log.info(
                "Loaded Hugging Face "
                f"dataset: {path}"
            )

            context.log.info(
                "Dataset metadata summary: "
                f"type={dataset_metadata.get('dataset_type')}, "
                f"streaming={dataset_metadata.get('streaming')}"
            )

            return Output(
                value=dataset,
                metadata={
                    "path": path,
                    "config": resolved_config,
                    "split": split,
                    "partition_key": (
                        context.partition_key
                        if context.has_partition_key
                        else None
                    ),
                    **dataset_metadata,
                },
            )

        return _asset

    return decorator
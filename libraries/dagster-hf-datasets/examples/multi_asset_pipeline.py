"""
Streaming Runtime Pipeline Example

This example shows how to process very large datasets using Hugging Face's streaming.
Streaming lets you work with data that doesn’t fit in memory by using IterableDatasets.
These datasets are only temporary and not saved. When you convert them into a regular
Dataset, Dagster stores it through the IO manager. Downstream assets then use this
saved Dataset.

Pipeline Stages:
1. sampled_c4: Stream C4 dataset and deterministically sample rows
2. filtered_c4: Remove short/malformed text examples
3. normalized_c4: Standardize text formatting and enrich with metadata
4. publish_golden_c4: Publish curated dataset to Hugging Face Hub

It is necessary to authenticate with Hugging Face via `hf auth login` before running.
"""

from __future__ import annotations

from itertools import islice

from datasets import (
    Dataset,
    load_dataset,
)

from dagster import (
    Definitions,
    asset,
    materialize,
)

from dagster_hf_datasets._export import (
    HFDatasetPublisher,
)
from dagster_hf_datasets._metadata import (
    build_dataset_metadata,
)
from dagster_hf_datasets.io_manager import (
    HFParquetIOManager,
)
from dagster_hf_datasets.resources import (
    HuggingFaceResource,
)


@asset(
    group_name="golden_c4_pipeline",
    io_manager_key="hf_parquet_io_manager",
)
def sampled_c4() -> Dataset:
    """
    Runtime-only streaming ingestion asset.

    Demonstrates:
    - Hugging Face streaming ingestion
    - runtime-only IterableDataset usage
    - deterministic sampling
    - conversion into persistable Dataset

    Important:
    The IterableDataset is NEVER persisted.
    Only the resulting Dataset artifact
    is materialized by Dagster.
    """

    streaming_dataset = load_dataset(
        "allenai/c4",
        "en",
        split="train",
        streaming=True,
    )

    sampled_rows = list(
        islice(
            streaming_dataset,
            500,
        )
    )

    dataset = Dataset.from_list(sampled_rows)

    metadata = build_dataset_metadata(
        dataset,
        path="allenai/c4",
    )

    print("\nStreaming Runtime Metadata:\n")

    for key, value in metadata.items():
        print(f"{key}: {value}")

    return dataset


@asset
def filtered_c4(
    sampled_c4: Dataset,
) -> Dataset:
    """
    Remove malformed and short examples.
    """

    def valid_example(
        example: dict,
    ) -> bool:
        text = example.get(
            "text",
            "",
        ).strip()

        return len(text.split()) >= 50

    return sampled_c4.filter(valid_example)


@asset
def normalized_c4(
    filtered_c4: Dataset,
) -> Dataset:
    """
    Normalize text formatting and
    produce ML-ready examples.
    """

    def normalize(
        example: dict,
    ) -> dict:
        text = example["text"].strip().replace("\n", " ")

        return {
            "text": text,
            "text_length": len(text),
            "url": example.get(
                "url",
                "unknown",
            ),
        }

    return filtered_c4.map(normalize)


@asset
def publish_golden_c4(
    normalized_c4: Dataset,
) -> str:
    """
    Publish processed dataset
    to Hugging Face Hub.
    """

    publisher = HFDatasetPublisher(
        repo_id=("AINovice2005/" "golden-c4-demo-v2"),
        private=False,
    )

    hub_url = publisher.publish(
        dataset=normalized_c4,
        source_dataset="allenai/c4",
        source_revision="main",
        description=(
            "Processed streaming C4 dataset " "with deterministic preprocessing."
        ),
        processing_steps=[
            "Streaming ingestion",
            "Deterministic sampling",
            "Removed short examples",
            "Normalized text formatting",
        ],
        metadata={
            "pipeline": ("golden_c4_pipeline"),
            "processing_mode": ("runtime_streaming"),
        },
    )

    print("\nDataset published successfully.")

    print(f"Hub URL: {hub_url}")

    return hub_url


resources = {
    "huggingface": HuggingFaceResource(
        cache_dir=".hf_cache",
        offline=False,
    ),
    "hf_parquet_io_manager": (
        HFParquetIOManager(
            base_dir=".dagster_hf_storage",
        )
    ),
}


defs = Definitions(
    assets=[
        sampled_c4,
        filtered_c4,
        normalized_c4,
        publish_golden_c4,
    ],
    resources=resources,
)


if __name__ == "__main__":
    print("\n=== Runtime-Only Streaming " "C4 Pipeline ===\n")

    print(
        "Pipeline semantics:\n"
        "- runtime-only streaming ingestion\n"
        "- no IterableDataset persistence\n"
        "- Dataset materialization boundary\n"
        "- parquet-backed downstream assets\n"
        "- metadata enrichment\n"
        "- Hugging Face Hub publishing\n"
    )

    result = materialize(
        [
            sampled_c4,
            filtered_c4,
            normalized_c4,
            publish_golden_c4,
        ],
        resources=resources,
    )

    print("\n=== Pipeline Result ===\n")

    if result.success:
        print("Pipeline completed successfully.")

    else:
        print("Pipeline execution failed.")

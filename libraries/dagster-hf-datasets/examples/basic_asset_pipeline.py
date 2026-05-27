"""
Basic Asset Pipeline Example

This example demonstrates the core workflow for integrating Hugging Face datasets
with Dagster using the dagster-hf-datasets package.

Pipeline Stages:
1. Define a Hugging Face resource with caching configuration
2. Create a Parquet IO Manager for persistent data storage
3. Declare a dataset asset that automatically materializes the data
4. Materialize the asset and inspect the resulting metadata

Make sure you have the Hugging Face CLI installed and authenticated before running this example.
"""

from __future__ import annotations

from datasets import (
    load_dataset,
)

from dagster import (
    Definitions,
    materialize,
)

from dagster_hf_datasets._metadata import (
    build_dataset_metadata,
)
from dagster_hf_datasets.assets import (
    hf_dataset_asset,
)
from dagster_hf_datasets.io_manager import (
    HFParquetIOManager,
)
from dagster_hf_datasets.resources import (
    HuggingFaceResource,
)


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


@hf_dataset_asset(
    path="nyu-mll/glue",
    config="qqp",
    split="train",
    group_name="huggingface_datasets",
    io_manager_key="hf_parquet_io_manager",
)
def glue_qqp_train():
    """
    Materialize the GLUE QQP training split
    as a Dagster asset backed by a
    Hugging Face Dataset.

    Demonstrates:
    - dataset materialization
    - parquet persistence
    - metadata enrichment
    - Hugging Face Hub observability
    """
    pass


defs = Definitions(
    assets=[
        glue_qqp_train,
    ],
    resources=resources,
)


def describe_metadata() -> None:
    """
    Demonstrate upgraded metadata extraction.
    """

    dataset = load_dataset(
        "nyu-mll/glue",
        "qqp",
        split="train",
    )

    metadata = build_dataset_metadata(
        dataset,
        path="nyu-mll/glue",
    )

    print("\n" "Dataset Metadata\n" "----------------\n")

    for key, value in metadata.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    describe_metadata()

    result = materialize(
        assets=[
            glue_qqp_train,
        ],
        resources=resources,
    )

    if result.success:
        print("\nSuccessfully materialized " "GLUE QQP training dataset asset.")

    else:
        raise RuntimeError("Dagster asset materialization failed.")

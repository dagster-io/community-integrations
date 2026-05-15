from datasets import Dataset

from dagster import (
    Definitions,
    asset,
    materialize,
)

from dagster_hf_datasets._export import (
    HFDatasetPublisher,
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


@hf_dataset_asset(
    path="nyu-mll/glue",
    config="qqp",
    split="train",
    group_name="golden_glue_pipeline",
    io_manager_key="hf_parquet_io_manager",
)
def raw_glue_qqp():
    """
    Load the raw GLUE QQP training split
    from the Hugging Face Hub.
    """
    ...


@asset
def deduplicated_glue_qqp(
    raw_glue_qqp: Dataset,
) -> Dataset:
    """
    Remove duplicate question pairs.
    """

    seen = set()

    def unique_example(example: dict) -> bool:
        key = (
            example["question1"],
            example["question2"],
        )

        if key in seen:
            return False

        seen.add(key)
        return True

    return raw_glue_qqp.filter(unique_example)


@asset
def filtered_glue_qqp(
    deduplicated_glue_qqp: Dataset,
) -> Dataset:
    """
    Remove malformed and short examples.
    """

    def valid_example(example: dict) -> bool:
        q1 = example["question1"]
        q2 = example["question2"]

        return (
            q1 is not None
            and q2 is not None
            and len(q1.split()) >= 5
            and len(q2.split()) >= 5
        )

    return deduplicated_glue_qqp.filter(
        valid_example
    )


@asset
def golden_glue_qqp(
    filtered_glue_qqp: Dataset,
) -> Dataset:
    """
    Normalize text formatting and
    produce a curated "golden" dataset.
    """

    def normalize(example: dict) -> dict:
        return {
            "question1": (
                example["question1"]
                .strip()
                .lower()
            ),
            "question2": (
                example["question2"]
                .strip()
                .lower()
            ),
            "label": example["label"],
        }

    return filtered_glue_qqp.map(normalize)


@asset
def publish_golden_glue(
    golden_glue_qqp: Dataset,
) -> str:
    """
    Publish the processed dataset
    to the Hugging Face Hub.
    """

    print(
        "\nPreparing dataset publication..."
    )
    print(
        f"Dataset rows: "
        f"{len(golden_glue_qqp)}"
    )

    publisher = HFDatasetPublisher(
        repo_id=(
            "AINovice2005/"
            "golden-glue-qqp"
        ),
        private=False,
    )

    hub_url = publisher.publish(
        dataset=golden_glue_qqp,
        source_dataset="nyu-mll/glue",
        source_revision="main",
        description=(
            "Curated GLUE QQP dataset "
            "with deduplication, filtering, "
            "and normalization applied."
        ),
        processing_steps=[
            "Removed duplicate question pairs",
            "Removed malformed examples",
            "Filtered short questions",
            "Normalized text formatting",
        ],
        metadata={
            "task": (
                "duplicate-question-detection"
            ),
            "source_config": "qqp",
            "pipeline": (
                "golden_dataset_pipeline"
            ),
        },
    )

    print(
        "\nDataset successfully pushed "
        "to the Hugging Face Hub."
    )
    print(f"Hub URL: {hub_url}")

    return hub_url


defs = Definitions(
    assets=[
        raw_glue_qqp,
        deduplicated_glue_qqp,
        filtered_glue_qqp,
        golden_glue_qqp,
        publish_golden_glue,
    ],
    resources={
        "huggingface": HuggingFaceResource(
            cache_dir=".hf_cache",
            offline=False,
        ),
        "hf_parquet_io_manager": (
            HFParquetIOManager(
                base_dir=".dagster_hf_storage",
            )
        ),
    },
)


if __name__ == "__main__":
    print(
        "\n=== Dagster Hugging Face "
        "Golden Dataset Pipeline ===\n"
    )

    print(
        "This example demonstrates:\n"
        "- Hugging Face dataset ingestion\n"
        "- Dagster asset orchestration\n"
        "- dataset transformations\n"
        "- parquet-backed persistence\n"
        "- lineage-aware processing\n"
        "- Hugging Face Hub publishing\n"
    )

    print(
        "Make sure you are authenticated "
        "with Hugging Face before running:\n"
    )

    print("    hf auth login\n")

    result = materialize(
        [
            raw_glue_qqp,
            deduplicated_glue_qqp,
            filtered_glue_qqp,
            golden_glue_qqp,
            publish_golden_glue,
        ],
        resources={
            "huggingface": HuggingFaceResource(
                cache_dir=".hf_cache",
                offline=False,
            ),
            "hf_parquet_io_manager": (
                HFParquetIOManager(
                    base_dir=".dagster_hf_storage",
                )
            ),
        },
    )

    print("\n=== Pipeline Result ===\n")

    if result.success:
        print(
            "Pipeline completed successfully."
        )
    else:
        print(
            "Pipeline execution failed."
        )

    print(
        "\nYou can also visualize these "
        "assets in the Dagster UI:\n"
    )

    print("    dagster dev\n")
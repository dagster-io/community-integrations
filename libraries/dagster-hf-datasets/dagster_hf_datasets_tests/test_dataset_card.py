import re

import pytest
from freezegun import freeze_time

from dagster_hf_datasets._export._dataset_card import (
    DatasetCardBuilder,
)


# ============================================================
# Helpers
# ============================================================


def build_card(
    *,
    metadata=None,
    processing_steps=None,
    source_revision="main",
):
    builder = DatasetCardBuilder(
        dataset_name="test-dataset",
        source_dataset="imdb",
        source_revision=source_revision,
        description="Test dataset card",
        processing_steps=processing_steps,
        metadata=metadata,
    )

    return builder.build()


# ============================================================
# Core Build Tests
# ============================================================


@freeze_time("2025-01-01")
def test_build_minimal_card():
    card = build_card()

    assert card.startswith("---")
    assert "# test-dataset" in card

    assert "Test dataset card" in card

    assert "- Dataset: `imdb`" in card
    assert "- Revision: `main`" in card

    assert "Generated with: `dagster-hf-datasets`" in card
    assert "2025-01-01" in card


@freeze_time("2025-01-01")
def test_build_contains_all_major_sections():
    card = build_card()

    expected_sections = [
        "## Dataset Summary",
        "## Source Dataset",
        "## Pipeline Semantics",
        "## Processing Lineage",
        "## Reproducibility",
        "## Metadata",
        "## Usage",
        "## Provenance",
    ]

    for section in expected_sections:
        assert section in card


# ============================================================
# YAML Frontmatter
# ============================================================


def test_yaml_frontmatter_contains_required_tags():
    builder = DatasetCardBuilder(
        dataset_name="test-dataset",
        source_dataset="imdb",
    )

    yaml = builder._build_yaml_frontmatter()

    assert "tags:" in yaml

    assert "- dagster" in yaml
    assert "- huggingface" in yaml
    assert "- datasets" in yaml
    assert "- lineage-tracking" in yaml

    assert "source_datasets:" in yaml
    assert "- imdb" in yaml


def test_yaml_frontmatter_adds_streaming_tag():
    builder = DatasetCardBuilder(
        dataset_name="test-dataset",
        source_dataset="imdb",
        metadata={
            "source_streaming": True,
        },
    )

    yaml = builder._build_yaml_frontmatter()

    assert "streaming-ingestion" in yaml


def test_yaml_frontmatter_adds_pipeline_tag():
    builder = DatasetCardBuilder(
        dataset_name="test-dataset",
        source_dataset="imdb",
        metadata={
            "pipeline": "etl",
        },
    )

    yaml = builder._build_yaml_frontmatter()

    assert "- etl" in yaml


# ============================================================
# Dataset Summary
# ============================================================


def test_dataset_summary_renders_scalar_fields():
    builder = DatasetCardBuilder(
        dataset_name="test-dataset",
        source_dataset="imdb",
        metadata={
            "num_rows": 100,
            "dataset_type": "Dataset",
        },
    )

    summary = builder._build_dataset_summary()

    assert "- Num Rows: `100`" in summary
    assert "- Dataset Type: `Dataset`" in summary


def test_dataset_summary_renders_feature_list():
    builder = DatasetCardBuilder(
        dataset_name="test-dataset",
        source_dataset="imdb",
        metadata={
            "features": [
                "text",
                "label",
            ]
        },
    )

    summary = builder._build_dataset_summary()

    assert "- Features:" in summary
    assert "  - `text`" in summary
    assert "  - `label`" in summary


def test_dataset_summary_empty_fallback():
    builder = DatasetCardBuilder(
        dataset_name="test-dataset",
        source_dataset="imdb",
    )

    summary = builder._build_dataset_summary()

    assert summary == "- No dataset summary available."


# ============================================================
# Pipeline Semantics
# ============================================================


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (True, "streaming"),
        (False, "materialized"),
    ],
)
def test_source_ingestion_mode_rendering(
    value,
    expected,
):
    builder = DatasetCardBuilder(
        dataset_name="test-dataset",
        source_dataset="imdb",
        metadata={
            "source_streaming": value,
        },
    )

    section = builder._build_pipeline_semantics()

    assert expected in section


def test_pipeline_semantics_rendering():
    builder = DatasetCardBuilder(
        dataset_name="test-dataset",
        source_dataset="imdb",
        metadata={
            "execution_mode": "streaming",
            "dataset_type": ("IterableDataset"),
            "pipeline_mode": ("distributed"),
        },
    )

    section = builder._build_pipeline_semantics()

    assert "Execution Mode: `streaming`" in section

    assert "Materialized Artifact Type: " "`IterableDataset`" in section

    assert "Pipeline Mode: `distributed`" in section


# ============================================================
# Processing Lineage
# ============================================================


def test_processing_steps_render_in_order():
    card = build_card(
        processing_steps=[
            "normalize",
            "tokenize",
            "filter",
        ]
    )

    assert card.index("normalize") < card.index("tokenize") < card.index("filter")


def test_processing_steps_empty_fallback():
    builder = DatasetCardBuilder(
        dataset_name="test-dataset",
        source_dataset="imdb",
    )

    section = builder._build_processing_steps()

    assert section == "- No processing steps recorded."


# ============================================================
# Reproducibility
# ============================================================


def test_reproducibility_renders_revision_and_fingerprint():
    builder = DatasetCardBuilder(
        dataset_name="test-dataset",
        source_dataset="imdb",
        source_revision="abc123",
        metadata={
            "fingerprint": "fp-001",
        },
    )

    section = builder._build_reproducibility_section()

    assert "Source Dataset Revision: " "`abc123`" in section

    assert "Fingerprint: `fp-001`" in section


# ============================================================
# Metadata Classification
# ============================================================


def test_metadata_section_classifies_sections():
    builder = DatasetCardBuilder(
        dataset_name="test-dataset",
        source_dataset="imdb",
        metadata={
            "custom_runtime": "x",
            "pipeline": "etl",
            "hub_downloads": 1000,
        },
    )

    section = builder._build_metadata_section()

    assert "### Runtime Metadata" in section

    assert "### Pipeline Metadata" in section

    assert "### Hub Metadata" in section


def test_summary_keys_not_duplicated_in_metadata():
    builder = DatasetCardBuilder(
        dataset_name="test-dataset",
        source_dataset="imdb",
        metadata={
            "num_rows": 100,
            "features": ["text"],
            "custom_runtime": "x",
        },
    )

    section = builder._build_metadata_section()

    assert "Num Rows" not in section

    assert "Features" not in section

    assert "Custom Runtime" in section


# ============================================================
# Metadata Rendering
# ============================================================


def test_render_metadata_list():
    builder = DatasetCardBuilder(
        dataset_name="test-dataset",
        source_dataset="imdb",
    )

    rendered = builder._render_metadata_line(
        "labels",
        ["a", "b"],
    )

    assert "- **Labels**:" in rendered
    assert "  - `a`" in rendered
    assert "  - `b`" in rendered


def test_render_metadata_dict():
    builder = DatasetCardBuilder(
        dataset_name="test-dataset",
        source_dataset="imdb",
    )

    rendered = builder._render_metadata_line(
        "config",
        {
            "batch_size": 32,
        },
    )

    assert "- **Config**:" in rendered

    assert "  - **batch_size**: `32`" in rendered


# ============================================================
# Formatting
# ============================================================


def test_format_key():
    formatted = DatasetCardBuilder._format_key("execution_mode")

    assert formatted == "Execution Mode"


# ============================================================
# Markdown Stability
# ============================================================


@freeze_time("2025-01-01")
def test_full_build_markdown_structure():
    card = build_card(
        metadata={
            "num_rows": 100,
            "features": [
                "text",
                "label",
            ],
            "execution_mode": ("streaming"),
            "pipeline": "etl",
            "hub_downloads": 500,
            "fingerprint": "abc123",
        },
        processing_steps=[
            "normalize",
            "tokenize",
        ],
    )

    assert card.count("---") >= 2

    assert card.count("## Dataset Summary") == 1

    assert card.count("## Metadata") == 1

    assert "```python" in card

    assert "load_dataset(" in card

    assert (
        re.search(
            r"Generated at: `2025-01-01",
            card,
        )
        is not None
    )

from __future__ import annotations

from datetime import (
    UTC,
    datetime,
)
from typing import Any


class DatasetCardBuilder:
    """
    Lightweight lineage-aware Hugging Face
    dataset card builder.

    Focus areas:
    - provenance
    - reproducibility
    - transformation lineage
    - runtime metadata observability
    - lightweight ML dataset documentation

    Explicit non-goals:
    - advanced templating systems
    - governance workflows
    - synchronization orchestration
    """

    HUB_METADATA_KEYS = {
        "hub_downloads",
        "hub_likes",
        "hub_tags",
        "hub_private",
        "hub_gated",
        "dataset_size_bytes",
        "download_size_bytes",
    }

    PIPELINE_METADATA_KEYS = {
        "pipeline",
        "pipeline_mode",
        "processing_mode",
        "processing_type",
        "source_streaming",
    }

    REPRODUCIBILITY_KEYS = {
        "fingerprint",
        "revision",
    }

    SUMMARY_KEYS = {
        "dataset_type",
        "num_rows",
        "features",
        "execution_mode",
        "streaming",
    }

    def __init__(
        self,
        *,
        dataset_name: str,
        source_dataset: str,
        source_revision: str | None = None,
        description: str | None = None,
        processing_steps: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.dataset_name = dataset_name
        self.source_dataset = source_dataset
        self.source_revision = source_revision
        self.description = description
        self.processing_steps = processing_steps or []
        self.metadata = metadata or {}

    def build(self) -> str:
        """
        Build lineage-aware Hugging Face
        dataset card.
        """

        generated_at = datetime.now(UTC).isoformat()

        yaml_frontmatter = self._build_yaml_frontmatter()

        dataset_summary = self._build_dataset_summary()

        pipeline_semantics = self._build_pipeline_semantics()

        processing_section = self._build_processing_steps()

        metadata_section = self._build_metadata_section()

        reproducibility_section = self._build_reproducibility_section()

        usage_section = self._build_usage_section()

        return f"""---
{yaml_frontmatter}
---

# {self.dataset_name}

{self.description or "Processed dataset generated with dagster-hf-datasets."}

## Dataset Summary

{dataset_summary}

## Source Dataset

- Dataset: `{self.source_dataset}`
- Revision: `{self.source_revision or "unknown"}`

## Pipeline Semantics

{pipeline_semantics}

## Processing Lineage

{processing_section}

## Reproducibility

{reproducibility_section}

## Metadata

{metadata_section}

## Usage

{usage_section}

## Provenance

- Generated at: `{generated_at}`
- Generated with: `dagster-hf-datasets`
- Lineage tracking enabled
"""

    def _build_yaml_frontmatter(
        self,
    ) -> str:
        """
        Build Hugging Face dataset card
        YAML metadata block.
        """

        tags = [
            "dagster",
            "huggingface",
            "datasets",
            "lineage-tracking",
        ]

        if self.metadata.get(
            "source_streaming",
            False,
        ):
            tags.append("streaming-ingestion")

        pipeline = self.metadata.get("pipeline")

        if pipeline:
            tags.append(str(pipeline))

        tag_lines = "\n".join(f"- {tag}" for tag in tags)

        return f"""language:
- en

tags:
{tag_lines}

source_datasets:
- {self.source_dataset}

generated_by:
- dagster-hf-datasets
"""

    def _build_dataset_summary(
        self,
    ) -> str:
        """
        Build dataset summary section.
        """

        lines: list[str] = []

        for key in self.SUMMARY_KEYS:
            value = self.metadata.get(key)

            if value is None:
                continue

            if key == "features":
                lines.append("- Features:")

                if isinstance(value, list):
                    lines.extend(f"  - `{feature}`" for feature in value)

                elif isinstance(
                    value,
                    dict,
                ):
                    for split, features in value.items():
                        lines.append(f"  - {split}:")

                        for feature in features:
                            lines.append(f"    - `{feature}`")

                continue

            lines.append(f"- {self._format_key(key)}: " f"`{value}`")

        if not lines:
            return "- No dataset summary available."

        return "\n".join(lines)

    def _build_pipeline_semantics(
        self,
    ) -> str:
        """
        Build pipeline semantics section.
        """

        lines: list[str] = []

        source_streaming = self.metadata.get("source_streaming")

        if source_streaming is not None:
            lines.append(
                "- Source Ingestion Mode: "
                f"`{'streaming' if source_streaming else 'materialized'}`"
            )

        execution_mode = self.metadata.get("execution_mode")

        if execution_mode:
            lines.append("- Execution Mode: " f"`{execution_mode}`")

        dataset_type = self.metadata.get("dataset_type")

        if dataset_type:
            lines.append("- Materialized Artifact Type: " f"`{dataset_type}`")

        pipeline_mode = self.metadata.get("pipeline_mode")

        if pipeline_mode:
            lines.append("- Pipeline Mode: " f"`{pipeline_mode}`")

        if not lines:
            return "- No pipeline semantics available."

        return "\n".join(lines)

    def _build_processing_steps(
        self,
    ) -> str:
        """
        Build processing lineage section.
        """

        if not self.processing_steps:
            return "- No processing steps recorded."

        return "\n".join(f"- {step}" for step in self.processing_steps)

    def _build_reproducibility_section(
        self,
    ) -> str:
        """
        Build reproducibility metadata.
        """

        lines: list[str] = []

        lines.append(
            f"- Source Dataset Revision: " f"`{self.source_revision or 'unknown'}`"
        )

        for key in self.REPRODUCIBILITY_KEYS:
            value = self.metadata.get(key)

            if value is None:
                continue

            lines.append(f"- {self._format_key(key)}: " f"`{value}`")

        if not lines:
            return "- No reproducibility metadata."

        return "\n".join(lines)

    def _build_metadata_section(
        self,
    ) -> str:
        """
        Build structured metadata summary.
        """

        if not self.metadata:
            return "- No additional metadata."

        sections = []

        runtime_lines = []
        pipeline_lines = []
        hub_lines = []

        for key, value in self.metadata.items():
            if key in self.SUMMARY_KEYS:
                continue

            if key in (self.REPRODUCIBILITY_KEYS):
                continue

            rendered = self._render_metadata_line(
                key,
                value,
            )

            if key in self.HUB_METADATA_KEYS:
                hub_lines.append(rendered)

            elif key in (self.PIPELINE_METADATA_KEYS):
                pipeline_lines.append(rendered)

            else:
                runtime_lines.append(rendered)

        if runtime_lines:
            sections.append("### Runtime Metadata\n\n" + "\n".join(runtime_lines))

        if pipeline_lines:
            sections.append("### Pipeline Metadata\n\n" + "\n".join(pipeline_lines))

        if hub_lines:
            sections.append("### Hub Metadata\n\n" + "\n".join(hub_lines))

        return "\n\n".join(sections)

    def _build_usage_section(
        self,
    ) -> str:
        """
        Build dataset usage examples.
        """

        return f"""```python
from datasets import load_dataset

dataset = load_dataset(
    "{self.dataset_name}"
)
```"""

    def _render_metadata_line(
        self,
        key: str,
        value: Any,
    ) -> str:
        """
        Render metadata values safely.
        """

        formatted_key = self._format_key(key)

        if isinstance(value, list):
            rendered = "\n".join(f"  - `{item}`" for item in value)

            return f"- **{formatted_key}**:\n" f"{rendered}"

        if isinstance(value, dict):
            rendered = "\n".join(f"  - **{k}**: `{v}`" for k, v in value.items())

            return f"- **{formatted_key}**:\n" f"{rendered}"

        return f"- **{formatted_key}**: " f"`{value}`"

    @staticmethod
    def _format_key(
        key: str,
    ) -> str:
        """
        Format metadata keys into
        readable display names.
        """

        return key.replace("_", " ").strip().title()

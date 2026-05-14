from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


class DatasetCardBuilder:
    """
    Lightweight Hugging Face dataset card builder.

    Focus areas:
    - provenance
    - reproducibility
    - transformation lineage
    - lightweight metadata documentation

    Explicit non-goals:
    - advanced templating systems
    - governance workflows
    - synchronization orchestration
    """

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
        self.processing_steps = (
            processing_steps or []
        )
        self.metadata = metadata or {}

    def build(self) -> str:
        """
        Build lightweight Hugging Face dataset card.
        """
        generated_at = datetime.now(
            UTC
        ).isoformat()

        processing_section = (
            self._build_processing_steps()
        )

        metadata_section = (
            self._build_metadata_section()
        )

        return f"""---
language:
- en

tags:
- dagster
- huggingface
- datasets
- lineage-tracking

source_datasets:
- {self.source_dataset}

generated_by:
- dagster-hf-datasets

---

# {self.dataset_name}

{self.description or "Processed dataset generated with dagster-hf-datasets."}

## Source Dataset

- Dataset: `{self.source_dataset}`
- Revision: `{self.source_revision or "unknown"}`

## Processing Lineage

{processing_section}

## Metadata

{metadata_section}

## Provenance

- Generated at: `{generated_at}`
- Generated with: `dagster-hf-datasets`
"""

    def _build_processing_steps(self) -> str:
        """
        Build processing lineage section.
        """
        if not self.processing_steps:
            return (
                "- No processing steps recorded."
            )

        return "\n".join(
            f"- {step}"
            for step in self.processing_steps
        )

    def _build_metadata_section(self) -> str:
        """
        Build metadata summary section.
        """
        if not self.metadata:
            return "- No additional metadata."

        return "\n".join(
            f"- **{key}**: `{value}`"
            for key, value in self.metadata.items()
        )
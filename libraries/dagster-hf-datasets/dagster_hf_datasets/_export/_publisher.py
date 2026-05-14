from __future__ import annotations

from pathlib import Path
from typing import Any

from datasets import Dataset
from huggingface_hub import create_repo

from dagster_hf_datasets._export._dataset_card import (
    DatasetCardBuilder,
)


class HFDatasetPublisher:
    """
    Lightweight Hugging Face dataset publisher.

    Responsibilities:
    - push processed datasets to HF Hub
    - optionally generate lightweight dataset cards

    Explicit non-goals:
    - synchronization orchestration
    - advanced governance
    - dataset lifecycle automation
    """

    def __init__(
        self,
        *,
        repo_id: str,
        private: bool = False,
        exist_ok: bool = True,
    ) -> None:
        self.repo_id = repo_id
        self.private = private
        self.exist_ok = exist_ok

    def publish(
        self,
        *,
        dataset: Dataset,
        source_dataset: str,
        source_revision: str | None = None,
        description: str | None = None,
        processing_steps: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        create_dataset_card: bool = True,
        commit_message: str = (
            "Upload dataset via dagster-hf-datasets"
        ),
    ) -> str:
        """
        Publish dataset to Hugging Face Hub.

        Returns:
            HF dataset repository URL.
        """
        create_repo(
            repo_id=self.repo_id,
            repo_type="dataset",
            private=self.private,
            exist_ok=self.exist_ok,
        )

        if create_dataset_card:
            self._upload_dataset_card(
                source_dataset=source_dataset,
                source_revision=source_revision,
                description=description,
                processing_steps=processing_steps,
                metadata=metadata,
            )

        dataset.push_to_hub(
            repo_id=self.repo_id,
            commit_message=commit_message,
        )

        return (
            f"https://huggingface.co/datasets/"
            f"{self.repo_id}"
        )

    def _upload_dataset_card(
        self,
        *,
        source_dataset: str,
        source_revision: str | None,
        description: str | None,
        processing_steps: list[str] | None,
        metadata: dict[str, Any] | None,
    ) -> None:
        """
        Generate temporary README.md dataset card
        and upload alongside dataset.
        """
        card_builder = DatasetCardBuilder(
            dataset_name=self.repo_id,
            source_dataset=source_dataset,
            source_revision=source_revision,
            description=description,
            processing_steps=processing_steps,
            metadata=metadata,
        )

        card_content = card_builder.build()

        temp_card_path = Path("README.md")

        temp_card_path.write_text(
            card_content,
            encoding="utf-8",
        )
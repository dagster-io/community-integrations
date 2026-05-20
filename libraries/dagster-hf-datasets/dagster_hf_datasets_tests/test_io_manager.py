from unittest.mock import MagicMock

import pandas as pd
import pytest
from dagster import (
    AssetKey,
    build_input_context,
    build_output_context,
)
from dagster._core.test_utils import (
    instance_for_test,
)
from datasets import (
    Dataset,
    IterableDataset,
)

from dagster_hf_datasets.io_manager._io_manager import (
    HFParquetIOManager,
)


# ============================================================
# Fixtures
# ============================================================


@pytest.fixture
def dagster_instance():
    with instance_for_test() as instance:
        yield instance


@pytest.fixture
def io_manager(temp_dir):
    return HFParquetIOManager(base_dir=str(temp_dir))


@pytest.fixture
def output_context(
    dagster_instance,
):
    context = build_output_context(
        asset_key=AssetKey(["test_asset"]),
        instance=dagster_instance,
    )

    context.add_output_metadata = MagicMock()

    return context


@pytest.fixture
def input_context(
    output_context,
    dagster_instance,
):
    return build_input_context(
        upstream_output=output_context,
        instance=dagster_instance,
    )


@pytest.fixture
def tiny_dataframe():
    return pd.DataFrame(
        {
            "text": [
                "a",
                "b",
            ],
            "score": [0.1, 0.2],
        }
    )


# ============================================================
# Output Path Resolution
# ============================================================


def test_get_output_path(
    io_manager,
    output_context,
    temp_dir,
):
    path = io_manager._get_output_path(output_context)

    expected = temp_dir / "test_asset"

    assert path == expected


# ============================================================
# Dataset Persistence
# ============================================================


def test_handle_output_persists_dataset(
    io_manager,
    output_context,
    tiny_dataset,
    temp_dir,
):
    io_manager.handle_output(
        output_context,
        tiny_dataset,
    )

    persisted_path = temp_dir / "test_asset"

    assert persisted_path.exists()

    loaded = Dataset.load_from_disk(str(persisted_path))

    assert loaded.num_rows == 3

    assert loaded.column_names == [
        "text",
        "label",
    ]


def test_load_input_restores_dataset(
    io_manager,
    output_context,
    input_context,
    tiny_dataset,
):
    io_manager.handle_output(
        output_context,
        tiny_dataset,
    )

    loaded = io_manager.load_input(input_context)

    assert isinstance(
        loaded,
        Dataset,
    )

    assert loaded.num_rows == 3


# ============================================================
# DataFrame Persistence
# ============================================================


def test_handle_output_persists_dataframe(
    io_manager,
    output_context,
    tiny_dataframe,
    temp_dir,
):
    io_manager.handle_output(
        output_context,
        tiny_dataframe,
    )

    parquet_path = temp_dir / "test_asset.parquet"

    assert parquet_path.exists()


def test_load_input_restores_dataframe(
    io_manager,
    output_context,
    input_context,
    tiny_dataframe,
):
    io_manager.handle_output(
        output_context,
        tiny_dataframe,
    )

    loaded = io_manager.load_input(input_context)

    assert isinstance(
        loaded,
        pd.DataFrame,
    )

    assert list(loaded.columns) == [
        "text",
        "score",
    ]

    assert len(loaded) == 2


# ============================================================
# IterableDataset Handling
# ============================================================


def test_iterable_dataset_not_persisted(
    io_manager,
    output_context,
):
    iterable_dataset = IterableDataset.from_generator(
        lambda: iter(
            [
                {"x": 1},
                {"x": 2},
            ]
        )
    )

    io_manager.handle_output(
        output_context,
        iterable_dataset,
    )

    persisted_path = io_manager._get_output_path(output_context)

    assert not persisted_path.exists()


# ============================================================
# Error Handling
# ============================================================


def test_handle_output_unsupported_type_raises(
    io_manager,
    output_context,
):
    with pytest.raises(
        TypeError,
        match="Unsupported object type",
    ):
        io_manager.handle_output(
            output_context,
            {"invalid": "object"},
        )


def test_load_input_missing_artifact_raises(
    io_manager,
    input_context,
):
    with pytest.raises(
        FileNotFoundError,
        match="No persisted artifact found",
    ):
        io_manager.load_input(input_context)


def test_get_input_path_without_upstream_output_raises(
    io_manager,
):
    context = build_input_context()

    with pytest.raises(
        ValueError,
        match=("Upstream output context " "is required"),
    ):
        io_manager._get_input_path(context)


# ============================================================
# Metadata Validation
# ============================================================


def test_dataset_metadata_emitted(
    io_manager,
    output_context,
    tiny_dataset,
):
    io_manager.handle_output(
        output_context,
        tiny_dataset,
    )

    output_context.add_output_metadata.assert_called_once()

    metadata = output_context.add_output_metadata.call_args[0][0]

    assert metadata["format"].value == "huggingface_dataset"

    assert metadata["streaming"].value is False

    assert metadata["rows"].value == 3

    assert metadata["columns"].value == 2


def test_dataframe_metadata_emitted(
    io_manager,
    output_context,
    tiny_dataframe,
):
    io_manager.handle_output(
        output_context,
        tiny_dataframe,
    )

    metadata = output_context.add_output_metadata.call_args[0][0]

    assert metadata["format"].value == "pandas_parquet"

    assert metadata["streaming"].value is False

    assert metadata["rows"].value == 2

    assert metadata["columns"].value == 2


def test_iterable_dataset_metadata_emitted(
    io_manager,
    output_context,
):
    iterable_dataset = IterableDataset.from_generator(lambda: iter([{"x": 1}]))

    io_manager.handle_output(
        output_context,
        iterable_dataset,
    )

    metadata = output_context.add_output_metadata.call_args[0][0]

    assert metadata["format"].value == "huggingface_iterable_dataset"

    assert metadata["streaming"].value is True

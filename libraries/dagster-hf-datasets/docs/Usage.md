# Dagster Hugging Face Datasets

`dagster-hf-datasets` provides lightweight Dagster integrations for Hugging Face Datasets library.

## What You Get

- **`HuggingFaceResource`** — Configurable Dagster resource that wraps `datasets.load_dataset` with built-in support for authentication, caching, and offline mode.
- **`hf_dataset_asset` and `hf_multi_asset`** — Decorators that turn HF datasets into Dagster assets with automatic metadata, split handling, and partition support.
- **`HFParquetIOManager`** — Persists datasets to disk using `save_to_disk()` for `datasets.Dataset` and Parquet format for `pandas.DataFrame`.

## Installation

```bash
pip install dagster-hf-datasets
```

## Quick Start

```python
from dagster import job
from dagster_hf_datasets import HuggingFaceResource, hf_dataset_asset, HFParquetIOManager

@hf_dataset_asset(path="imdb")
def imdb_dataset():
    pass  # Function body is ignored; resource loads the dataset

@job
def my_job():
    imdb_dataset()

# Register resources and IO managers
resources = {"huggingface": HuggingFaceResource(token=None)}
io_managers = {"io_manager": HFParquetIOManager(base_dir=".dagster_hf_storage")}
```

## Common Configurations

**Private datasets:** Set `token`, `token_path`, or the `HF_TOKEN` environment variable.

**Offline mode:** Set `offline=True` on `HuggingFaceResource`.

**Streaming datasets:** Use `streaming=True` to load as `IterableDataset`. These are not persisted by the IO manager.

**Multiple dataset splits:** Use `hf_multi_asset` to automatically expand a `DatasetDict` into separate Dagster outputs.

## Publishing Datasets to Hugging Face Hub

Use `HFDatasetPublisher` to push processed datasets to the Hub:

```python
from dagster_hf_datasets._export._publisher import HFDatasetPublisher

publisher = HFDatasetPublisher(
    repo_id="my-org/processed-data",
    token="${HF_TOKEN}",  # Optional; falls back to HF_TOKEN env var
    private=False,
    exist_ok=True,
)
```

### Parameters

- `repo_id` — Hub repository ID (e.g., `username/my-dataset`)
- `token` — HF API token (optional)
- `private` — Create private repo when `True`
- `exist_ok` — Don't fail if repo already exists

## Examples

See [examples](libraries/dagster-hf-datasets/examples) for complete working pipelines and multi-asset patterns.

### Publishing a Dataset

```python
# dataset must be a datasets.Dataset (not DatasetDict)
url = publisher.publish(
    dataset=dataset,
    source_dataset="imdb",
    source_revision="main",
    description="Processed IMDB dataset with text-cleaning and deduplication.",
    processing_steps=["clean_text", "deduplicate"],
    metadata={"pipeline_version": "1.2.0"},
    create_dataset_card=True,
)

print(f"Published to {url}")
```

`publish()` will:
1. Create the Hub repository
2. Generate and upload a dataset card (README.md)
3. Push the dataset using `dataset.push_to_hub()`

Use `processing_steps`, `metadata`, `source_dataset`, and `source_revision` to track data lineage.

## Metadata and Lineage

The integration automatically emits rich metadata for observability and lineage tracking.

### Where Metadata Appears

- **Asset materializations** — `hf_dataset_asset` and `hf_multi_asset` include normalized metadata
- **IO Manager** — `HFParquetIOManager` adds storage metadata (path, format, row count, columns, fingerprint)

### Common Metadata Keys

| Key | Type | Description |
|-----|------|-------------|
| `dataset_type` | string | Runtime type (e.g., `Dataset`, `DatasetDict`) |
| `streaming` | boolean | `True` for streaming/iterable datasets |
| `execution_mode` | string | `materialized` or `lazy_streaming` |
| `num_rows` | int \| dict | Row count per split (or `None` for streaming) |
| `num_shards` | int | Number of data shards (optional) |
| `features` | list \| dict | Column names per split |
| `fingerprint` | string | Dataset fingerprint for reproducibility |
| `revision` | string | Dataset version/tag |
| `hub_downloads` | int | Hub download count |
| `hub_likes` | int | Hub likes count |
| `hub_private` | boolean | Whether dataset is private |
| `dataset_size_bytes` | int | Total dataset size |

### Lineage Best Practices

1. **Include provenance** — Set `source_dataset` and `source_revision` in `publisher.publish()` for dataset cards
2. **Track fingerprints** — Persist `fingerprint` and `revision` to detect upstream changes
3. **Document transformations** — Use `processing_steps` for human-readable transformation history
4. **Cross-reference metadata** — Combine IO manager metadata with Hub dataset cards for complete lineage views



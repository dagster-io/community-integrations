# API Reference

## Public API

All main classes and functions are exported from `dagster_hf_datasets`:

```python
from dagster_hf_datasets import (
    HuggingFaceResource,
    hf_dataset_asset,
    hf_multi_asset,
    HFParquetIOManager,  # Also available from dagster_hf_datasets.io_manager
)
```

## HuggingFaceResource

Configurable resource that wraps `datasets.load_dataset` for loading Hugging Face datasets in Dagster ops and assets.

### Configuration

| Parameter | Type | Description |
|-----------|------|-------------|
| `token` | `str \| None` | HF API token (explicit) |
| `token_path` | `str \| None` | Path to file containing token |
| `cache_dir` | `str \| None` | Directory for dataset cache |
| `offline` | `bool` | Enable offline mode (sets `HF_HUB_OFFLINE=1`) |

### Methods

```python
# Load a dataset
load_dataset(path, config=None, split=None, revision=None, streaming=False, **kwargs)
# Returns the loaded dataset object

# Get row counts (None for streaming datasets)
get_num_rows(dataset) -> int | dict | None

# Get column information
get_features(dataset) -> datasets.Features | dict

# Get reproducibility fingerprint
get_fingerprint(dataset) -> str | dict | None

# Extract dataset version
get_revision(dataset) -> str | None
```

## hf_dataset_asset

Decorator for creating Dagster assets backed by Hugging Face datasets.

### Parameters

**Required:**
- `path` — HF dataset identifier or local script path

**Optional:**
- `config` — Dataset configuration name
- `split` — Specific split to load (single-asset mode)
- `revision` — Dataset revision/branch/tag/commit
- `streaming` — Load as streaming dataset (`bool`)
- `name`, `group_name`, `key_prefix`, `metadata`, `tags`, `io_manager_key`, `partitions_def` — Standard Dagster asset parameters

### Behavior

- Loads dataset via `HuggingFaceResource` (must be available in context)
- Returns `Output` with dataset + metadata (path, config, split, rows, columns, fingerprint, type, streaming)
- With `partitions_def`: supports partition-driven config/revision via `HFPartitionMapping`

### Example

```python
from dagster import job
from dagster_hf_datasets import hf_dataset_asset, HuggingFaceResource

@hf_dataset_asset(path="imdb", split="train")
def imdb_train():
    pass

@job
def load_imdb():
    imdb_train()
```

## hf_multi_asset

Decorator for creating multi-assets from Hugging Face `DatasetDict` (creates one output per split).

### Parameters

**Required:**
- `path` — HF dataset identifier

**Optional:**
- `config` — Dataset configuration name
- `revision` — Dataset revision/branch/tag/commit
- `streaming` — Load as streaming datasets (`bool`)
- `group_name`, `key_prefix`, `metadata`, `op_tags`, `io_manager_key`, `partitions_def` — Standard Dagster multi-asset parameters

### Behavior

- Resolves available splits at decoration time using `datasets.get_dataset_split_names()`
- Creates one output per split with split-specific metadata
- Supports selective materialization via `can_subset=True`
- Raises `ValueError` if split resolution fails

### Example

```python
from dagster import job
from dagster_hf_datasets import hf_multi_asset, HuggingFaceResource

@hf_multi_asset(path="imdb")
def imdb_splits():
    pass  # Automatically creates train, test, unsupervised outputs

@job
def load_imdb():
    imdb_splits()
```

## HFParquetIOManager

IO manager for persisting datasets to disk.

### Configuration

```python
HFParquetIOManager(base_dir=".dagster_hf_storage")
```

### Supported Types

| Type | Behavior |
|------|----------|
| `datasets.Dataset` | Saved with `save_to_disk()` |
| `pandas.DataFrame` | Saved as Parquet (`.parquet`) |
| `datasets.IterableDataset` | Runtime-only (not persisted) |

### Methods

```python
# Persist output and attach metadata
handle_output(context, obj)

# Load persisted data
load_input(context) -> Dataset | DataFrame
```

### Metadata Emitted

When persisting outputs:
- `path` — Storage path
- `format` — File format (parquet, disk)
- `rows` — Row count
- `columns` — Column names
- `fingerprint` — Dataset fingerprint
- `streaming` — Whether it's a streaming dataset

## Internal Utilities

- `HFPartitionMapping` — Maps Dagster partition keys to dataset `config` or `revision` (used internally with `partitions_def`)
- Export helpers — For building dataset cards and publishing (see `dagster_hf_datasets._export`)


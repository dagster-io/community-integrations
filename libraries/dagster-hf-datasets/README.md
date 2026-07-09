# Dagster-HF-Datasets

<p align="center">
  <img
    src="https://raw.githubusercontent.com/dagster-io/community-integrations/main/libraries/dagster-hf-datasets/docs/assets/dagster_readme_logo.jpg"
    alt="Dagster-HF-Datasets Logo"
    width="500"
  />
</p>

## Overview

Dagster-HF-Datasets integrates Hugging Face datasets with Dagster for building reproducible, observable data pipelines. Load datasets directly as Dagster assets, apply transformations, and publish results back to the Hub.

### Features

- **Hugging Face dataset assets** — Load any HF dataset as a Dagster asset with automatic metadata.
- **Streaming support** — Efficiently handle large datasets with runtime-only streaming mode.
- **Parquet persistence** — Auto-save datasets to disk for caching and versioning.
- **Metadata & lineage** — Rich metadata for observability and data lineage tracking.
- **Multi-asset pipelines** — Create split-aware assets from datasets with multiple splits.
- **Hub publishing** — Push processed datasets back to the Hugging Face Hub with dataset cards.

---

## Installation

```bash
pip install dagster-hf-datasets
```

## Development Install:

```bash
git clone https://github.com/dagster-io/community-integrations.git

cd libraries/dagster-hf-datasets

pip install -e .
```

---

## Examples

### Basic Asset Pipeline

Get started with a simple example of materializing a Hugging Face dataset as a Dagster asset:

See [examples/basic_asset_pipeline.py](https://github.com/dagster-io/community-integrations/blob/main/libraries/dagster-hf-datasets/examples/basic_asset_pipeline.py)

- Dataset materialization with `hf_dataset_asset`
- Parquet persistence via `HFParquetIOManager`
- Automatic metadata enrichment
- Hugging Face Hub observability

---

### Multi-Asset Streaming Pipeline

Process large datasets efficiently with runtime-only streaming ingestion:

See [examples/multi_asset_pipeline.py](https://github.com/dagster-io/community-integrations/blob/main/libraries/dagster-hf-datasets/examples/multi_asset_pipeline.py)

- Streaming dataset loading with `load_dataset(..., streaming=True)`
- Deterministic sampling of IterableDatasets
- Metadata extraction from streaming sources
- Conversion to persistent materialized artifacts

---

### Complete Dataset Pipeline

Build production-grade data pipelines with dataset cleaning, transformation and publishing:

See [examples/multi_asset_pipeline.py](https://github.com/dagster-io/community-integrations/blob/main/libraries/dagster-hf-datasets/examples/dataset_pipeline.py)

- Deduplication and filtering of raw data
- Text normalization and formatting
- Multi-step lineage-aware transformations
- Hugging Face Hub dataset publishing

---

## Documentation

- [Usage Guide](https://github.com/dagster-io/community-integrations/blob/main/libraries/dagster-hf-datasets/docs/Usage.md) — Quick start, configuration, publishing datasets to Hugging Face Hub, and metadata/lineage tracking
- [API Reference](https://github.com/dagster-io/community-integrations/blob/main/libraries/dagster-hf-datasets/docs/API.md) — Complete API documentation for `HuggingFaceResource`, asset decorators, and the IO manager

---

## Resources

- [Release Article](https://huggingface.co/blog/AINovice2005/dagster-hf-datasets) — Deep dive into the motivation, architecture, runtime lifecycle and patterns behind `dagster-hf-datasets`.

- [Official Dagster Documentation](https://docs.dagster.io/integrations/libraries/hf-datasets) — Installation instructions, features and end-to-end usage guide.

- [Examples on Hugging Face](https://huggingface.co/buckets/the-hf-stack/dagster-hf-datasets-examples) — Explore 10+ curated example pipelines.

---

## Development

### Test

```bash
make test
```

### Build

```bash
make build
```
---

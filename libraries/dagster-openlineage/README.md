# dagster-openlineage

Dagster integration for OpenLineage for automatic metadata collection. It provides an OpenLineage sensor that tails Dagster event logs, converts events to OpenLineage metadata, and emits them to a backend.

## Features

- **Metadata tracking** for Dagster jobs & ops lifecycle
- **OpenLineage sensor** for automatic event collection

## Requirements

- Python 3.10+
- Dagster >=1.6.9

## Installation

```bash
pip install dagster-openlineage
```

## Quick Start

Add the `openlineage_sensor` to your Dagster `Definitions`:

```python
from dagster import Definitions, job, op
from dagster_openlineage import openlineage_sensor

@op
def extract():
    return {"data": 1}

@op
def transform(data):
    return data["data"] + 1

@job
def my_job():
    transform(extract())

defs = Definitions(
    jobs=[my_job],
    sensors=[openlineage_sensor()]
)
```

## Usage

### Sensor Configuration

The sensor supports several configuration options:

| Option | Default | Description |
|--------|---------|-------------|
| `minimum_interval_seconds` | 300 | Minimum seconds between sensor evaluations |
| `record_filter_limit` | 30 | Max number of event logs to process per evaluation |
| `after_storage_id` | 0 | Starting storage ID for event processing |

**Custom configuration example:**

```python
defs = Definitions(
    jobs=[my_job],
    sensors=[
        openlineage_sensor(
            minimum_interval_seconds=60,
            record_filter_limit=60,
        )
    ]
)
```

**Resuming from a specific storage ID:**

Use `after_storage_id` to resume processing from a specific point in your event log. This is useful when you need to reprocess events or start from a known storage ID:

```python
defs = Definitions(
    jobs=[my_job],
    sensors=[openlineage_sensor(after_storage_id=500)]
)
```

### Configuration

#### Environment Variables

Set the following environment variables:

- `OPENLINEAGE_URL` - URL of the OpenLineage backend (required)
- `OPENLINEAGE_API_KEY` - Bearer token for authentication (optional)
- `OPENLINEAGE_NAMESPACE` - Default namespace (optional)

**Where to set these:**

- **User Repository Deployments**: Add variables to the repository where the sensor is defined
- **Otherwise**: Add variables to the Dagster Daemon

#### Important Notes

- **Single sensor per instance**: Only define one OpenLineage sensor per Dagster instance. Multiple sensors will emit duplicate job runs.

- **Non-sharded event log storage**: Use non-sharded event log storage. Sharded storage (like default `SqliteEventLogStorage`) may prevent the cursor from updating properly.


## Version Compatibility

This library supports Dagster versions `>=1.6.9`.

- **End users**: `pip install dagster-openlineage` respects `pyproject.toml` constraints, allowing any compatible Dagster version
- **CI builds**: Uses committed `uv.lock` for reproducible builds
- **Developers**: Can test specific Dagster versions by updating the lock file


## Development

### Setup

```bash
# Install dependencies
uv sync

# Run tests
make test
```

### Testing with Different Dagster Versions

```bash
# Install desired version
uv pip install dagster==1.XX

# Update lock file
uv lock --upgrade-package dagster==1.XX

# Run tests
make test

# Revert to default
uv lock
make test
```


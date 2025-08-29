# Dagster Dataform Integration

A Dagster integration for Google Cloud Dataform that provides asset definitions and workflow monitoring capabilities.

## Core Features

### Observability
- **Asset Discovery**: Automatically creates Dagster assets from Dataform compilation results. 
- **Asset Check Discovery**: Native functionality for ingesting Dataform SQL Assertions in repo as asset checks that are visible in the Dagster UI as asset checks on the assets they test. 
- **Workflow Monitoring**: Real-time monitoring of Dataform workflow invocations via sensors
- **Rich Metadata**: Asset metadata (retrieved at definition time) and materialization metadata (captured at runtime) offer a great degree of detail of remote BQ assets.
- **SQL Query Tracking**: Preserves and displays SQL queries with proper formatting
- **GCP Integration**: Seamless integration with Google Cloud Platform using default credentials

> [!NOTE]  
>  Everytime a code location is reloaded, a new compilation result is created for the targeted branch (environment). Therefore if changes are mode to the branch, all that is necessary to refresh Dataform Asset/Asset Check metadata in Dagster is to reload the code location.

### Orchestration
Orchestrate Dataform invocations remotely via configuration of a dagster schedule automation. This configuration offers more flexibility than release and workflow configurations through the GCP Console in Dataform, including settings like assertion schema.

### Alerting
- Developed with flexible alerting in mind; users can define their own Dagster jobs to pass at definition time that will be run upon Dataform asset execution failure or orchestration job failure.

## Installation

### Prerequisites

- Python 3.9 or higher
- Google Cloud Platform project with Dataform enabled
- Appropriate GCP credentials configured

### Install the Package

```bash
pip install dagster-dataform
```

Or install from source:

```bash
git clone <repository-url>
cd dagster-dataform
pip install -e .
```

## Configuration

### GCP Authentication

This integration uses Google Cloud's default application credentials. The following authentication methods are supported:

1. **Service Account Key File**:
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
   ```

2. **Application Default Credentials**:
   ```bash
   gcloud auth application-default login
   ```

3. **Workload Identity** (for GKE):
   - Configure workload identity in your Kubernetes cluster
   - The integration will automatically use the service account attached to the pod

4. **Compute Engine/Cloud Run Service Account**:
   - When running on GCP services, the integration will use the default service account

### Required Permissions

Your GCP credentials must have the following permissions:

- `dataform.compilationResults.list`
- `dataform.compilationResults.get`
- `dataform.workflowInvocations.list`
- `dataform.workflowInvocations.get`
- `bigquery.jobs.get` (for job status monitoring)

## Usage

### Basic Setup

Create a `definitions.py` file in your Dagster project:

```python
from dagster_dataform import (
    DataformRepositoryResource,
    create_dataform_workflow_invocation_sensor,
    create_dataform_orchestration_schedule,
)

from dagster import Definitions
from .jobs import dataform_workflow_invocation_failure_notification_job, dataform_asset_check_failure_notification_job

resource = DataformRepositoryResource(
    project_id="your-project",
    repository_id="your-repo",
    location="us-east4",
    environment="env", # github branch
    sensor_minimum_interval_seconds=30,
)

assets = resource.assets
asset_checks = resource.asset_checks

dataform_workflow_invocation_sensor = create_dataform_workflow_invocation_sensor(
    resource,
    inclusion_patterns=[
        r"^conform_.*",
        r"^stg_.*",
        r"^curated_.*",
        r"^consume_.*",
        r".*assertions.*",
    ],
    minutes_ago=20,
    workflow_invocation_failure_notification_job=dataform_workflow_invocation_failure_notification_job,
    asset_check_failure_notification_job=dataform_asset_check_failure_notification_job,
)

dataform_orchestration_schedule = create_dataform_orchestration_schedule(
    resource=resource,
    cron_schedule="*/2 * * * *",
    git_commitish=resource.environment,
)

defs = Definitions(
    assets=assets,
    asset_checks=asset_checks,
    sensors=[dataform_workflow_invocation_sensor],
    schedules=[dataform_orchestration_schedule],
)

```

For freshness checks to show up in the Dagster UI, add a `dagster.yaml` file at the root of your project with the following content:

```yaml
freshness:
  enabled: True
```

### Custom Alerting Jobs Definition
This integration contains placeholder alerting jobs that are triggered on workflow invocation action failures (asset materialization or SQL Assertion failures). Users can pass their own valid Dagster jobs to the Dataform polling sensor creator function. There are two types of jobs that can be passed:
- **workflow_invocation_failure_notification_job**: This job will be run for any workflow invocation action that *is not* an assertion with a terminal state != "SUCCEEDED"
- **asset_check_failure_notification_job**: This job will be run for any workflow invocation action that *is* an assertion with a terminal state != "SUCCEEDED"

There are a few constraints to be mindful of regarding these custome jobs:
- The `workflow_invocation_failure_notification_job` must be named "dataform_workflow_invocation_failure_notification_job" (and the op must be named "dataform_workflow_invocation_failure_notification_op")
- The `asset_check_failure_notification_job` must be named "dataform_asset_check_failure_notification_job" (and the op must be named "dataform_asset_check_failure_notification_op")

Below is an example of some valid custom jobs that can be passed to the sensor creation function:

```python
from dagster import OpExecutionContext, job, op
from dagster_msteams import MSTeamsResource
from dagster_dataform import DataformFailureNotificationOpConfig
from .utils import build_adaptive_card_payload

workflow_invocation_failure_channel_msteams_resource = MSTeamsResource(
    hook_url="**********",
)

asset_check_failure_channel_msteams_resource = MSTeamsResource(
    hook_url="**********",
)

@op
def dataform_workflow_invocation_failure_notification_op(context: OpExecutionContext, config: DataformFailureNotificationOpConfig):
    context.log.info("Constructing Adaptive Card")
    context.log.info("Sending Adaptive Card to microsoft teams")
    workflow_invocation_failure_channel_msteams_resource.get_client().post_message(payload=build_adaptive_card_payload(
        event_type="Dataform Workflow Invocation Action Failure",
        card_description="materialization of an asset in a Dataform workflow invocation",
        environment=config.environment,
        asset_name=config.asset_name,
        workflow_invocation_start_time_secs=config.workflow_invocation_start_time_secs,
        workflow_invocation_state=config.workflow_invocation_state,
        failure_reason=config.failure_reason,
    ))
    context.log.info("Adaptive Card sent to microsoft teams")

@job
def dataform_workflow_invocation_failure_notification_job():
    dataform_workflow_invocation_failure_notification_op()

@op
def dataform_asset_check_failure_notification_op(context: OpExecutionContext, config: DataformFailureNotificationOpConfig):
    context.log.info("Constructing Adaptive Card")
    context.log.info("Sending Adaptive Card to microsoft teams")
    asset_check_failure_channel_msteams_resource.get_client().post_message(payload=build_adaptive_card_payload(
        event_type="Dataform Asset Check Failure",
        card_description="execution of a Dataform SQL Assertion",
        environment=config.environment,
        asset_name=config.asset_name,
        workflow_invocation_start_time_secs=config.workflow_invocation_start_time_secs,
        workflow_invocation_state=config.workflow_invocation_state,
        failure_reason=config.failure_reason,
    ))
    context.log.info("Adaptive Card sent to microsoft teams")

@job
def dataform_asset_check_failure_notification_job():
    dataform_asset_check_failure_notification_op()

```

### Library Objects and Functions

This section describes the core objects and functions provided by the dagster-dataform integration, including their parameters, return types, and usage examples.

#### DataformRepositoryResource

The main resource class that provides access to Google Cloud Dataform services. This resource handles authentication, API calls, and data retrieval from your Dataform repository.

**Constructor Parameters:**
- `project_id` (str, required): Your Google Cloud Platform project ID
- `repository_id` (str, required): Your Dataform repository ID
- `location` (str, required): GCP region where Dataform is deployed (e.g., "us-central1", "us-east4")
- `environment` (str, required): Environment name that matches your Dataform branch (e.g., "dev", "prod", "main")
- `sensor_minimum_interval_seconds` (int, optional): Minimum polling interval for sensors in seconds (default: 120)

**Key Methods:** (These methods do not need to be called directly by the user, but for informational purposes here they are)
- `create_compilation_result()`: Creates a new compilation result in Dataform
- `create_workflow_invocation()`: Initiates a workflow execution
- `get_workflow_invocation_details()`: Retrieves status and details of workflow executions
- `list_compilation_results()`: Lists available compilation results
- `query_compilation_result_actions()`: Queries actions from compilation results
- `load_dataform_assets()`: Automatically discovers and creates Dagster assets from your Dataform compilation results. This function analyzes your Dataform repository and generates asset definitions with proper dependencies and metadata.
- `load_dataform_asset_checks()`: Automatically discovers and creates Dagster assets from your Dataform compilation results. This function analyzes your Dataform repository and generates asset definitions with proper dependencies and metadata.

**Properties**
- `assets`: List containing Dagster AssetSpec objects
- `asset_checks`: List containing Dagster AssetCheckDefinition objects

**Example:**
```python
resource = DataformRepositoryResource(
    project_id="my-gcp-project",
    repository_id="my-dataform-repo",
    location="us-central1",
    environment="dev",
    sensor_minimum_interval_seconds=30,
)

assets = resource.assets
asset_checks = resource.asset_checks
```

#### `create_dataform_workflow_invocation_sensor()`

Creates a Dagster sensor that monitors Dataform workflow invocations and triggers notifications or actions based on their status. The sensor polls for new workflow executions and creates asset materializations or observations.

**Parameters:**
- `resource` (DataformRepositoryResource, required): The configured Dataform repository resource
- `minutes_ago` (int, required): How far back in time to look for workflow invocations
- `exclusion_patterns` (List[str], optional): Regex patterns to exclude certain assets from monitoring (ex. assets from workspaces with table prefixes)
- `job` (JobDefinition, optional): Custom job to run when sensor observes a workflow invocation failure (default: creates a notification job)

**Returns:**
- `SensorDefinition`: A configured Dagster sensor that monitors workflow invocations

**Behavior:**
- Polls for workflow invocations within the specified time window
- Creates asset materializations for successful runs
- Creates asset observations for failed or in-progress runs
- Includes metadata like SQL queries, BigQuery job IDs, and execution status
- Can trigger custom jobs for failure handling

**Example:**
```python
sensor = create_dataform_workflow_invocation_sensor(
    resource=resource,
    minutes_ago=20,
    exclusion_patterns=[
        r"^staging_.*",
        r"^temp_.*",
    ],
)
```

#### `create_dataform_orchestration_schedule()`

Creates a Dagster schedule that orchestrates Dataform workflow executions on a cron schedule. This function provides programmatic control over when and how Dataform workflows are triggered, offering more flexibility than GCP Console configurations.

**Parameters:**
- `resource` (DataformRepositoryResource, required): The configured Dataform repository resource
- `cron_schedule` (str, required): Cron expression defining when to run the schedule (e.g., "0 0 * * *" for daily at midnight)
- `git_commitish` (str, required): Git branch, tag, or commit hash to use for compilation
- `default_database` (str, optional): Default database override for compilation
- `default_schema` (str, optional): Default schema override for compilation
- `default_location` (str, optional): Default location override for compilation
- `assertion_schema` (str, optional): Schema for data quality assertions
- `database_suffix` (str, optional): Suffix to append to database names
- `schema_suffix` (str, optional): Suffix to append to schema names
- `table_prefix` (str, optional): Prefix to prepend to table names
- `builtin_assertion_name_prefix` (str, optional): Prefix for built-in assertion names
- `vars` (Dict[str, Any], optional): Variables to pass to the Dataform compilation

**Returns:**
- `ScheduleDefinition`: A configured Dagster schedule that orchestrates Dataform workflows

**Features:**
- Creates compilation results with specified parameters
- Initiates workflow invocations automatically
- Monitors execution status with configurable timeout (30 minutes default)
- Handles success/failure scenarios with proper error reporting
- Provides detailed logging throughout the process

**Example:**
```python
schedule = create_dataform_orchestration_schedule(
    resource=resource,
    cron_schedule="0 2 * * *",  # Daily at 2 AM
    git_commitish="main",
    default_database="analytics",
    assertion_schema="data_quality",
    vars={"environment": "production"}
)
```

### Example Asset Metadata

Each asset includes rich metadata:

```python
{
    "Project ID": "your-gcp-project-id",
    "Dataset": "your_dataset",
    "Asset Name": "your_table_name",
    "Asset SQL Code": "```sql\nSELECT * FROM source_table\n```",
    "Docs Link": "https://your-docs-link"
}
```

## Running the Integration

### Start Dagster UI

```bash
dagster dev
```

### View Assets

1. Navigate to the Dagster UI
2. Go to the "Assets" tab
3. You'll see all your Dataform assets listed
4. Click on any asset to view its metadata, dependencies, and materialization history

### Monitor Workflows

1. Go to the "Sensors" tab
2. Find the `dataform_workflow_invocation_sensor`
3. The sensor will automatically poll for new workflow invocations
4. View materialization events in the asset history

## Development

### Running Tests

```bash
# Run all tests
uv run pytest

# Run specific test files
uv run pytest dagster_dataform_tests/test_utils.py
uv run pytest dagster_dataform_tests/test_resources.py
uv run pytest dagster_dataform_tests/test_dataform_orchestration_schedule.py
```

### Building the Package

```bash
# Install in development mode
uv sync --dev

# Build package
uv run python -m build
```

### Local Development

```bash
# Install in development mode
uv sync --dev

# Run with your local Dagster instance
uv run dagster dev
```

## Troubleshooting

### Common Issues

1. **Authentication Errors**:
   - Ensure `GOOGLE_APPLICATION_CREDENTIALS` is set correctly
   - Verify your service account has the required permissions
   - Check that the Dataform API is enabled in your GCP project

2. **No Assets Found**:
   - Verify your Dataform repository has been compiled
   - Check that the `environment` parameter matches your Dataform branch
   - Ensure the repository ID is correct

3. **Sensor Not Triggering**:
   - Check the sensor's minimum interval configuration
   - Verify that workflow invocations exist in the specified time window
   - Review sensor logs for any errors

### Debug Mode

Enable debug logging by setting the log level:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## License

[Add your license information here]

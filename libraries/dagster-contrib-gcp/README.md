# `dagster-contrib-gcp`

## Test

```sh
make test
```

## Build

```sh
make build
```

## Overview

This package provides integrations with Google Cloud Platform (GCP) services. It currently includes the following
integrations:

### Cloud Run

#### Cloud Run run launcher

Adds support for launching Dagster runs on Google Cloud Run. Usage is as follows:

1. Create a Cloud Run Job from your Dagster code location image to act as the run worker. If you require multiple
   environments/code locations, you can create multiple Cloud Run Jobs.
2. Add `dagster-contrib-gcp` to your Dagster webserver/daemon environment.
3. Add the following configuration to your Dagster instance YAML:

```yaml
run_launcher:
  module: dagster_contrib_gcp.cloud_run.run_launcher
  class: CloudRunRunLauncher
  config:
    project:
      env: GOOGLE_CLOUD_PROJECT
    region:
      env: GOOGLE_CLOUD_REGION
    job_name_by_code_location:
      my-code-location-1: my-cloud-run-job-1
      # Optional Configuration
      my-code-location-2: 
        name: my-cloud-run-job-2
        project_id: 
          secret_name: SOME_GCP_SECRET
        region:
          env: A_DIFFERENT_GOOGLE_CLOUD_REGION
```
#### Code Location Configuration
The following configurations are supported per code-location:

```yaml
# No customizations
my-code-location-1: my-cloud-run-job-1

# Environment Variable or Secrets Manager references
my-code-location-1: 
  name: my-cloud-run-job-1
  project_id:
    secret_name: A_GCP_SECRET_NAME
  region:
    env: SOME_ENVIRONMENT_VARIABLE

# Explicit in-line declaration
my-code-location-1: 
  name: my-cloud-run-job-1
  project_id: gcp_123
  region: us-central1

# Multi-container support - which container to override on run
my-code-location-1: 
  name: my-cloud-run-job-1
  project_id: gcp_123
  region: us-central1
  container_name: my-dagster-container-name
```

Additional steps may be required for configuring IAM permissions, etc. In particular:
- Ensure that the webserver/daemon environment has the necessary permissions to execute the Cloud Run jobs
- Ensure the webserver/daemon can access Secret Manager (if using code location configuration with Secret Manager)
- Ensure that the Cloud Run run worker jobs have the necessary permissions to execute your Dagster runs
See the [Cloud Run documentation](https://cloud.google.com/run/docs) for more information.

#### Cloud Run job executor

Adds support for launching each **step** of a run as its own Cloud Run Job execution, instead of
running all of a run's steps inside the single run worker container. It composes with the
`CloudRunRunLauncher` above: the run launcher launches the run worker as one Cloud Run execution,
and (only for jobs that select this executor) the run worker then launches each of its steps as
further Cloud Run executions on the **same** Cloud Run Job resource (same image; only the
container args differ: `execute_step` vs `execute_run`).

This is most useful for jobs that fan out into many batches via `DynamicOut`/mapped ops - each
mapped step invocation gets its own Cloud Run execution, and the executor's `max_concurrent`
config caps how many run concurrently, so a large backlog doesn't spawn hundreds of executions
at once.

```python
from dagster import DynamicOut, DynamicOutput, job, op
from dagster_contrib_gcp.cloud_run import cloud_run_job_executor


@op(out=DynamicOut(int))
def split(context):
    for i, batch in enumerate(get_batches()):
        yield DynamicOutput(batch, mapping_key=str(i))


@op
def process_batch(context, batch):
    ...


@job(executor_def=cloud_run_job_executor)
def my_job():
    split().map(process_batch)
```

Configure it with run config:

```yaml
execution:
  config:
    project:
      env: GOOGLE_CLOUD_PROJECT
    region:
      env: GOOGLE_CLOUD_REGION
    job_name: my-cloud-run-job-1
    # Optional
    container_name: my-dagster-container-name
    max_concurrent: 4
    step_timeout: 3600
    run_job_retry:
      wait: 10
      timeout: 300
```
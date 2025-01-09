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
      my-code-location-2: my-cloud-run-job-2
```

Additional steps may be required for configuring IAM permissions, etc. In particular:
- Ensure that the webserver/daemon environment has the necessary permissions to execute the Cloud Run jobs
- Ensure that the Cloud Run run worker jobs have the necessary permissions to execute your Dagster runs
See the [Cloud Run documentation](https://cloud.google.com/run/docs) for more information.
# dagster-teradata

A dagster module that provides integration with [Teradata Vantage](https://www.teradata.com/).

## Installation
The `dagster_teradata` module is available as a PyPI package - install with your preferred python 
environment manager.

```
source .venv/bin/activate
pip install dagster-teradata
```

## Example Usage

This offers seamless integration with Teradata Vantage, facilitating efficient workflows for data processing, management, 
and transformation. This module supports a range of scenarios, such as executing queries, managing tables, 
and integrating with cloud storage solutions like AWS S3 and Azure Data Lake Storage (ADLS). Additionally, 
it enables compute cluster management for Teradata Vantage Cloud Lake.

```python
import os
import pytest
from dagster import job, op, EnvVar
from dagster_teradata import TeradataResource

td_resource = TeradataResource(
    host=EnvVar("TERADATA_HOST"),
    user=EnvVar("TERADATA_USER"),
    password=EnvVar("TERADATA_PASSWORD"),
    database=EnvVar("TERADATA_DATABASE"),
)

def test_execute_query(tmp_path):
    @op(required_resource_keys={"teradata"})
    def example_test_execute_query(context):
        result = context.resources.teradata.execute_queries(
            ["select order_id from orders_24", "select order_id from orders_25"], True
        )
        context.log.info(result)

    @job(resource_defs={"teradata": td_resource})
    def example_job():
        example_test_execute_query()

    example_job.execute_in_process(resources={"teradata": td_resource})
```
```python
import os
import pytest
from dagster import job, op, EnvVar
from dagster_teradata import TeradataResource

td_resource = TeradataResource(
    host=EnvVar("TERADATA_HOST"),
    user=EnvVar("TERADATA_USER"),
    password=EnvVar("TERADATA_PASSWORD"),
    database=EnvVar("TERADATA_DATABASE"),
)

def test_drop_table(tmp_path):
    @op(required_resource_keys={"teradata"})
    def example_test_drop_table(context):
        result = context.resources.teradata.drop_table(["process_tmp1", "process_tmp2"])
        context.log.info(result)

    @job(resource_defs={"teradata": td_resource})
    def example_job():
        example_test_drop_table()

    example_job.execute_in_process(resources={"teradata": td_resource})
```

Here is another example of compute cluster management in Teradata VantageCloud Lake:

```python
import os

import pytest
from dagster import job, op, EnvVar
from dagster_teradata import teradata_resource

def test_create_teradata_compute_cluster(tmp_path):
    @op(required_resource_keys={"teradata"})
    def example_create_teradata_compute_cluster(context):
        """Args for create_teradata_compute_cluster():
        compute_profile_name: Name of the Compute Profile to manage.
        compute_group_name: Name of compute group to which compute profile belongs.
        query_strategy: Query strategy to use. Refers to the approach or method used by the
                Teradata Optimizer to execute SQL queries efficiently within a Teradata computer cluster.
                Valid query_strategy value is either 'STANDARD' or 'ANALYTIC'. Default at database level is STANDARD
        compute_map: ComputeMapName of the compute map. The compute_map in a compute cluster profile refers
                to the mapping of compute resources to a specific node or set of nodes within the cluster.
        compute_attribute: Optional attributes of compute profile. Example compute attribute
                MIN_COMPUTE_COUNT(1) MAX_COMPUTE_COUNT(5) INITIALLY_SUSPENDED('FALSE')
                   compute_attribute (str, optional): Additional attributes for compute profile. Defaults to None.
        """
        context.resources.teradata.create_teradata_compute_cluster(
            "ShippingCG01",
            "Shipping",
            "STANDARD",
            "TD_COMPUTE_MEDIUM",
            "MIN_COMPUTE_COUNT(1) MAX_COMPUTE_COUNT(1) INITIALLY_SUSPENDED('FALSE')",
        )

    @job(resource_defs={"teradata": teradata_resource})
    def example_job():
        example_create_teradata_compute_cluster()

    example_job.execute_in_process(
        run_config={
            "resources": {
                "teradata": {
                    "config": {
                        "host": EnvVar("TERADATA_HOST"),
                        "user": EnvVar("TERADATA_USER"),
                        "password": EnvVar("TERADATA_PASSWORD"),
                        "database": EnvVar("TERADATA_DATABASE"),
                    }
                }
            }
        }
    )
```

## Development

The `Makefile` provides the tools required to test and lint your local installation.

```sh
make test
make ruff
make check
```

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
## BTEQ Operator

The bteq_operator method enables execution of SQL statements or BTEQ (Basic Teradata Query) scripts using the Teradata BTEQ utility. 
It supports running commands either on the local machine or on a remote machine over SSH — in both cases, the BTEQ utility must be installed on the target system.

### Key Features

- Executes SQL provided as a string or from a script file (only one can be used at a time).
- Supports custom encoding for the script or session.
- Configurable timeout and return code handling.
- Remote execution supports authentication using a password or an SSH key.
- Works in both local and remote setups, provided the BTEQ tool is installed on the system where execution takes place.

> Ensure that the Teradata BTEQ utility is installed on the machine where the SQL statements or scripts will be executed.
>
> This could be:
>  * The local machine where Dagster runs the task, for local execution.
>  * The remote host accessed via SSH, for remote execution.
>  * If executing remotely, also ensure that an SSH server (e.g., sshd) is running and accessible on the remote machine.

### Parameters

- `sql`: SQL statement(s) to be executed using BTEQ. (optional, mutually exclusive with `file_path`)
- `file_path`: If provided, this file will be used instead of the `sql` content. This path represents remote file path when executing remotely via SSH, or local file path when executing locally. (optional, mutually exclusive with `sql`)
- `remote_host`: Hostname or IP address for remote execution. If not provided, execution is assumed to be local. *(optional)*  
- `remote_user`: Username used for SSH authentication on the remote host. Required if `remote_host` is specified.  
- `remote_password`: Password for SSH authentication. Optional, and used as an alternative to `ssh_key_path`.  
- `ssh_key_path`: Path to the SSH private key used for authentication. Optional, and used as an alternative to `remote_password`.  
- `remote_port`: SSH port number for the remote host. Defaults to `22` if not specified. *(optional)*
- `remote_working_dir`: Temporary directory location on the remote host (via SSH) where the BTEQ script will be transferred and executed. Defaults to `/tmp` if not specified. This is only applicable when `ssh_conn_id` is provided.
- `bteq_script_encoding`: Character encoding for the BTEQ script file. Defaults to ASCII if not specified.
- `bteq_session_encoding`: Character set encoding for the BTEQ session. Defaults to ASCII if not specified.
- `bteq_quit_rc`: Accepts a single integer, list, or tuple of return codes. Specifies which BTEQ return codes should be treated as successful, allowing subsequent tasks to continue execution.
- `timeout`: Timeout (in seconds) for executing the BTEQ command. Default is 600 seconds (10 minutes).
- `timeout_rc`: Return code to use if the BTEQ execution fails due to a timeout. To allow Ops execution to continue after a timeout, include this value in `bteq_quit_rc`. If not specified, a timeout will raise an exception and stop the Ops.

### Returns

- Output of the BTEQ execution, or `None` if no output was produced.

### Raises

- `ValueError`: For invalid input or configuration
- `DagsterError`: If BTEQ execution fails or times out

### Notes

- Either `sql` or `file_path` must be provided, but not both.
- For remote execution, provide either `remote_password` or `ssh_key_path` (not both).
- Encoding and timeout handling are customizable.
- Validates remote port and authentication parameters.

### Example Usage

```python
# Local execution with direct SQL
output = bteq_operator(sql="SELECT * FROM table;")

# Remote execution with file
output = bteq_operator(
    file_path="script.sql",
    remote_host="example.com",
    remote_user="user",
    ssh_key_path="/path/to/key.pem"
)
```

## Development

The `Makefile` provides the tools required to test and lint your local installation.

```sh
make test
make ruff
make check
```

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

## DDL Operator

The ddl_operator method executes DDL (Data Definition Language) statements on Teradata using the Teradata Parallel Transporter (TPT).
It supports both local and remote execution (via SSH), allowing you to manage Teradata schema objects seamlessly within Dagster pipelines.

### Key Features

- Executes one or more DDL statements in a single operation.
- Supports both local and remote execution via SSH.
- Handles custom error codes to allow continued workflow execution.
- Allows configuration of remote working directory and SSH credentials.
- Supports custom job naming for easier tracking and debugging.
- Provides robust validation for input parameters and execution environment.

> Ensure that the Teradata Parallel Transporter (TPT) is installed on the system where DDL execution will occur.
> This could be:
> * The local machine running Dagster (for local execution).
> * A remote host accessible via SSH (for remote execution).
> * For remote setups, ensure an SSH server (e.g., sshd) is active and reachable.

### Parameters

| Parameter | Type | Description | Required | Default |
|------------|------|-------------|-----------|----------|
| `ddl` | `list[str]` | List of DDL statements to be executed. Each entry must be a valid SQL string. | ✅ | — |
| `error_list` | `int` \| `List[int]` | Single integer or list of integers representing error codes to ignore. | ❌ | — |
| `remote_working_dir` | `str` | Directory on the remote host used as a working directory for temporary DDL files or logs. | ❌ | `/tmp` |
| `ddl_job_name` | `str` | A descriptive name for the DDL job to assist in tracking and logging. | ❌ | — |
| `remote_host` | `str` | Hostname or IP address of the remote machine where the DDL should run. If not specified, execution is local. | ❌ | — |
| `remote_user` | `str` | SSH username for authentication on the remote host. Required if `remote_host` is provided. | ❌ | — |
| `remote_password` | `str` | Password for SSH authentication. Used as an alternative to `ssh_key_path`. | ❌ | — |
| `ssh_key_path` | `str` | Path to the SSH private key file used for authentication. Used as an alternative to `remote_password`. | ❌ | — |
| `remote_port` | `int` | SSH port for the remote host. Must be within range `1–65535`. | ❌ | `22` |

### Returns

- **`Optional[int]`** — Return code from the DDL execution.  
  Returns `None` if no return code is applicable or produced.

### Raises

- **`ValueError`** — If input validation fails (e.g., missing DDL list, invalid SSH configuration, or bad port number).  
- **`Exception`** — If the DDL execution fails and the resulting error code is not part of the `error_list`.

### Notes

- The `ddl` parameter **must** be a non-empty list of valid SQL DDL statements.  
- For remote execution, either `remote_password` or `ssh_key_path` **must** be provided (but not both).  
- Invalid or missing SSH configuration parameters will raise validation errors before execution.  
- Ensures strong validation for parameters like SSH port and DDL syntax structure.

### Example Usage

### Local Execution with Multiple DDL Statements

```python
return_code = ddl_operator(
    ddl=[
        "CREATE TABLE employees (id INT, name VARCHAR(100));",
        "CREATE INDEX idx_name ON employees(name);"
    ]
)
```

### Remote Execution Using SSH Key Authentication

```python
return_code = ddl_operator(
    ddl=["DROP TABLE IF EXISTS sales;"],
    remote_host="td-server.example.com",
    remote_user="td_admin",
    ssh_key_path="/home/td_admin/.ssh/id_rsa",
    ddl_job_name="drop_sales_table"
)
```

### Remote Execution Using Password Authentication with Ignored Error Codes

```python
return_code = ddl_operator(
    ddl=["CREATE DATABASE reporting;"],
    remote_host="192.168.1.100",
    remote_user="teradata",
    remote_password="password123",
    error_list=[3807],  # Ignore 'database already exists' error
    ddl_job_name="create_reporting_db"
)
```

### Integration with Dagster

The `ddl_operator` can be used within Dagster assets or ops to automate schema management:

```python
from dagster import asset
from dagster_teradata import DdlOperator

@asset
def create_tables(context):
    context.resources.teradata.ddl_operator(
        ddl=[
            "CREATE TABLE sales (id INTEGER, amount DECIMAL(10,2));",
            "CREATE TABLE customers (cust_id INTEGER, name VARCHAR(100));"
        ]
    )
```

## TdLoadOperator

The `tdload_operator` method enables execution of **TDLoad** jobs for transferring data between Teradata tables or between Teradata and external files.  
It provides a unified interface to run **TDLoad** operations either **locally** or **remotely via SSH**, allowing data ingestion and export tasks to be seamlessly integrated within Dagster pipelines.

---

## Key Features

- Executes TDLoad operations for data import/export between Teradata and external files.  
- Supports both local and remote execution via SSH.  
- Allows custom job configuration, variable files, and TDLoad command options.  
- Handles multiple data source and target formats (e.g., CSV, TEXT, PARQUET).  
- Supports text delimiters for structured data handling.  
- Provides strong validation for remote execution parameters and ports.  

> Ensure that the **TDLoad utility** is installed on the machine where execution will occur.  
> 
> This could be:  
>  * The local machine running Dagster (for local execution).  
>  * A remote host accessible via SSH (for remote execution).  
>  * For remote setups, ensure that an SSH server (e.g., `sshd`) is active and reachable.

### Parameters

| Parameter | Type | Description | Required | Default |
|------------|------|-------------|-----------|----------|
| `source_table` | `str` | Name of the source table in Teradata. | ❌ | — |
| `select_stmt` | `str` | SQL `SELECT` statement for extracting data. | ❌ | — |
| `insert_stmt` | `str` | SQL `INSERT` statement for loading data. | ❌ | — |
| `target_table` | `str` | Name of the target table in Teradata. | ❌ | — |
| `source_file_name` | `str` | Source file path for input data. | ❌ | — |
| `target_file_name` | `str` | Target file path for output data. | ❌ | — |
| `source_format` | `str` | Format of the source file (e.g., CSV, TEXT). | ❌ | — |
| `source_text_delimiter` | `str` | Field delimiter for the source file. | ❌ | — |
| `target_format` | `str` | Format of the target file (e.g., CSV, TEXT). | ❌ | — |
| `target_text_delimiter` | `str` | Field delimiter for the target file. | ❌ | — |
| `tdload_options` | `str` | Additional TDLoad options to customize execution. | ❌ | — |
| `tdload_job_name` | `str` | Name assigned to the TDLoad job. | ❌ | — |
| `tdload_job_var_file` | `str` | Path to the TDLoad job variable file. | ❌ | — |
| `remote_working_dir` | `str` | Directory on remote host for temporary TDLoad job files. | ❌ | `/tmp` |
| `remote_host` | `str` | Hostname or IP of the remote machine for execution. | ❌ | — |
| `remote_user` | `str` | Username for SSH authentication on the remote host. Required if `remote_host` is specified. | ❌ | — |
| `remote_password` | `str` | Password for SSH authentication. Alternative to `ssh_key_path`. | ❌ | — |
| `ssh_key_path` | `str` | Path to SSH private key used for authentication. Alternative to `remote_password`. | ❌ | — |
| `remote_port` | `int` | SSH port for the remote connection (1–65535). | ❌ | `22` |


### Return Value

- **`int | None`** — Return code from the TDLoad operation.  
  Returns `None` if no return code is applicable or produced.

### Raises

- **`ValueError`** — For invalid parameter combinations (e.g., both password and key provided, invalid port, or missing SSH credentials).  
- **`Exception`** — If the TDLoad execution fails for reasons not covered by user configuration.

## Notes

- Either a `source_table`/`select_stmt` **or** a `source_file_name` must be provided as the input source.  
- Either a `target_table`/`insert_stmt` **or** a `target_file_name` must be provided as the target.  
- For remote execution, only one authentication method can be used — either `remote_password` or `ssh_key_path`.  
- The `remote_port` must be a valid port number between `1` and `65535`.  
- The `tdload_job_name` and `tdload_job_var_file` can help manage reusable or parameterized load operations.

### Examples

#### 1. File to Table
Load data from a file into a Teradata table.

```python
@asset
def file_to_table_load(context):
    context.resources.teradata.tdload_operator(
        source_file_name="/data/customers.csv",
        target_table="customers",
        source_format="Delimited",
        source_text_delimiter="|"
    )
```

#### 2. Table to File
Export data from a Teradata table to a file.

```python
@asset
def table_to_file_export(context):
    context.resources.teradata.tdload_operator(
        source_table="sales",
        target_file_name="/data/sales_export.csv",
        target_format="Delimited",
        target_text_delimiter=","
    )
```

#### 3. Table to Table
Transfer data between Teradata tables.

```python
@asset
def table_to_table_transfer(context):
    context.resources.teradata.tdload_operator(
        source_table="staging_sales",
        target_table="prod_sales",
        insert_stmt="INSERT INTO prod_sales SELECT * FROM staging_sales WHERE amount > 1000"
    )
```

#### 4. Custom SELECT to File
Export data using a custom SQL query.

```python
@asset
def custom_export(context):
    context.resources.teradata.tdload_operator(
        select_stmt="SELECT customer_id, SUM(amount) FROM sales GROUP BY customer_id",
        target_file_name="/data/customer_totals.csv"
    )
```

### Advanced Usage

#### Using Job Variable Files

```python
@asset
def use_job_var_file(context):
    context.resources.teradata.tdload_operator(
        tdload_job_var_file="/config/load_job_vars.txt",
        tdload_options="-j my_load_job"
    )
```

#### Remote Execution with SSH Key

```python
@asset
def remote_tpt_operation(context):
    context.resources.teradata.tdload_operator(
        source_file_name="/remote/data/input.csv",
        target_table="remote_table",
        remote_host="td-prod.company.com",
        remote_user="tdadmin",
        ssh_key_path="/home/user/.ssh/td_key",
        remote_working_dir="/tmp/tpt_work"
    )
```

### Custom TPT Options

```python
@asset
def custom_tpt_options(context):
    context.resources.teradata.tdload_operator(
        source_table="large_table",
        target_file_name="/data/export.csv",
        tdload_options="-f CSV -m 4 -s ,",  # Format: CSV, 4 streams, comma separator
        tdload_job_name="custom_export_job"
    )
```

### Error Handling

Both operators include comprehensive error handling:

- **Connection errors:** Automatic retry and cleanup  
- **SSH failures:** Graceful degradation with detailed logging  
- **TPT execution errors:** Proper exit code handling and error messages  
- **Resource cleanup:** Automatic cleanup of temporary files  

### Best Practices

- Use `error_list` for idempotent DDL operations  
- Specify `remote_working_dir` for SSH operations  
- Use meaningful job names for monitoring and debugging  
- Test with small datasets before scaling up  
- Monitor TPT job logs for performance optimization  

## Development

The `Makefile` provides the tools required to test and lint your local installation.

```sh
make test
make ruff
make check
```

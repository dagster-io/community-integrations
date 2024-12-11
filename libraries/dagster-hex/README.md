# Dagster Hex Integration

### Installation

To install the library, use pip alongside your existing Dagster environment.

```bash
pip install dagster-hex
```

### Usage

Create a `HexResource` specifying the `api_key`, then, use that resource in our assets.

```python
import os
import dagster as dg

from dagster_hex.resources import HexResource 


@dg.asset
def custom_hex_asset(
    context: dg.AssetExecutionContext,
    hex: HexResource
) -> dg.MaterializeResult:
    project_id = "abcdefgh-1234-abcd-1234-abcdefghijkl"
    context.log.info(f"Running Hex project {project_id}")
    response = hex.run_project(
        project_id,
        inputs={
            "message": "Hello, World!"
        }
    )
    return dg.MaterializeResult(
        metadata={
            "data": response.get("data")
        }
    )


defs = Definitions(
    assets=[custom_hex_asset],
    resources={
        "hex": HexResource(
            api_key=dg.EnvVar("HEX_API_KEY")
        )
    }
)
```

#### Legacy

##### Ops

The `hex_project_op` will call the Hex API to run a project until it completes.

```python
import os
import dagster as dg

from dagster_hex.ops import hex_project_op
from dagster_hex.resources import hex_resource


hex_resource = HexResource(
    api_key=dg.EnvVar("HEX_API_KEY")
)

run_hex_op = hex_project_op.configured({
    "name": "run_hex_project_op",
    "project_id": "abcdefgh-1234-abcd-1234-abcdefghijkl"
})

@job(resource_defs={"hex": my_resource})
def hex_job():
    run_hex_op()
```

##### Asset Materializations

Ops will return an `AssetMaterialization`  with the following keys:

```
run_url	
run_status_url	
trace_id	
run_id	
elapsed_time
notifications	
```

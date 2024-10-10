# Hex Dagster Library

### Installation

To install the library, use pip alongside your existing Dagster environment.

```bash
pip install dagster-hex
```

### Configuration

First, you'll need to specify your Hex API Token key as a resource.

```python
# resources.py
from dagster_hex.resources import hex_resource 
import os

API_KEY = os.getenv['DAGSTER_PROD_API']
my_resource = hex_resource.configured({'api_key': API_KEY})
```

### Ops

The `hex_project_op` will call the Hex API to run a project until it completes.

```python
from dagster_hex.resources import hex_resource
from dagster import job
from dagster_hex.ops import hex_project_op

API_KEY = 'abc123'
PROJ_ID = 'i-love-uuids'

my_resource = hex_resource.configured({'api_key': API_KEY})
run_hex_op = hex_project_op.configured({"project_id": PROJ_ID},
                                       name='run_job')

@job(resource_defs={"hex": my_resource})
def hex_job():
    run_hex_op()
```

### Asset Materializations

Ops will return an `AssetMaterialization`  with the following keys:

```
run_url	
run_status_url	
trace_id	
run_id	
elapsed_time
notifications	
```

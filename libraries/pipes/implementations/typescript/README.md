# dagster-pipes-typescript

A pipes implementation for the typescript programming language.

Allows integration between any typescript process, and the dagster orchestrator.

## Usage

### Prerequisites

- [install node and npm](https://nodejs.org/en/download)
- install the typescript compiler (`npm install -g typescript`)

### Installation

`dagster_pipes_typescript` is available as an npm package:

```sh
npm install dagster_pipes_typescript
```

### Example

> In addition to the examples below - you can find an example dagster deployment with a typescript pipes asset in the `example-project/` directory.

After installing the dagster_pipes_typescript npm package, your typescript process can import it to integrate with dagster.

```typescript
import * as dagster_pipes from 'dagster_pipes_typescript';

using context = dagster_pipes.openDagsterPipes()

context.logger.info("this is a log message from the external process")
context.logger.warning("This is an example warning from the external process")

context.reportAssetMaterialization(
    {
        "row_count": 100
    }
)
context.reportAssetCheck(
    "example_typescript_check", true
)
```

And, in the dagster orchestration code, add the following:

```python
import subprocess
from pathlib import Path
import dagster as dg

@dg.asset(
    check_specs=[
        dg.AssetCheckSpec(
            name="example_typescript_check",
            asset="example_typescript_asset"
        )
    ]
)
def example_typescript_asset(
    context: dg.AssetExecutionContext, 
    pipes_subprocess_client: dg.PipesSubprocessClient
) -> dg.MaterializeResult:
    external_script_path = dg.file_relative_path(__file__, "../external_typescript_code/")
    
    subprocess.run(["npm install"],
                    cwd=external_script_path).check_returncode()
    subprocess.run(["tsc"],
                   cwd=external_script_path).check_returncode()

    return pipes_subprocess_client.run(
        command=["node", Path(external_script_path)],
        context=context,
    ).get_materialize_result()


defs = dg.Definitions(
    assets=[example_typescript_asset],
    resources={
        "pipes_subprocess_client": dg.PipesSubprocessClient()
    },
)

```

## Development

### Compiling the development version

```
npm install && npm run build
```

### Testing

```
npm test
```

### Linting

```
npm run lint
```
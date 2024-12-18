# dagster-pipes-rust

A pipes implementation for the [Rust](https://www.rust-lang.org/) programming language.

Get full observability into your Rust workloads when orchestrating through Dagster. With this light weight interface, you can retrieve data directly from the Dagster context, report asset materializations, report asset checks, provide structured logging, end more.

[![Crates.io](https://img.shields.io/crates/v/dagster_pipes_rust.svg)](https://crates.io/crates/dagster_pipes_rust)

## Usage

### Installation

```sh
cargo add dagster_pipes_rust
```

### Example

An example project can be found in [./example-dagster-pipes-rust-project](./example-dagster-pipes-rust-project).

In this project there exists a `rust_processing_jobs` binary, which demonstrates how to use the Dagster context to report materializations to Dagster through the `context.report_asset_materialization` method.

```rust
use dagster_pipes_rust::open_dagster_pipes;
use serde_json::json;

fn main() {
    let mut context = open_dagster_pipes();
    let metadata = json!({"row_count": {"raw_value": 100, "type": "int"}});
    context.report_asset_materialization("example_rust_subprocess_asset", metadata);
}
```

<img width="1355" alt="image" src="https://github.com/user-attachments/assets/ddc8c261-3e96-4e82-ad4c-723dd6b3dece">

It also demonstrates how to run the Rust binary in a subprocess from Dagster. Note, that it's also possible to launch processes in external compute environments like Kubernetes.

```python
import shutil

import dagster as dg


@dg.asset(
    group_name="pipes",
    kinds={"rust"},
)
def example_rust_subprocess_asset(
    context: dg.AssetExecutionContext, pipes_subprocess_client: dg.PipesSubprocessClient
) -> dg.MaterializeResult:
    """Demonstrates running Rust binary in a subprocess."""
    cmd = [shutil.which("cargo"), "run"]
    cwd = dg.file_relative_path(__file__, "../rust_processing_jobs")
    return pipes_subprocess_client.run(
        command=cmd,
        cwd=cwd,
        context=context,
    ).get_materialize_result()


defs = dg.Definitions(
    assets=[example_rust_subprocess_asset],
    resources={
        "pipes_subprocess_client": dg.PipesSubprocessClient(
            context_injector=dg.PipesEnvContextInjector(),
        )
    },
)
```


## Contributing

### Pipes Schema

We use [jsonschema](https://json-schema.org/) to define the pipes protocol and [quicktype](https://quicktype.io/) to generate the Rust structs. Currently, the json schemas live in `jsonschema/pipes` but they should be hosted/defined in a centralized repository in the future.

To generate the Rust structs, make sure to install quicktype with `npm install -g quicktype`. Then run:

```bash
cd community-integrations/pipes
make jsonschema_rust
```

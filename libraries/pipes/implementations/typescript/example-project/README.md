# example-project (typescript pipes)

This is an example project showcasing the usage of the typescript dagster-pipes library.

The dagster asset in the orchestrator process `example_typescript_asset`, utilizes 
`PipesSubprocessClient` to run the code of `external_typescript_code` in a pipes context.

## Running

Note that `external_typescript_code` depends on the local (dev) version of `dagster_pipes_typescript`.
You can change the dependency to the latest `npm` release with `npm uninstall dagster_pipes_typescript && npm install dagster_pipes_typescript`

```bash
# Compile the local (dev) version of dagster_pipes_typescript
npm install .. && npm run build '-p ..'
# Run dagster orchestration locally
uv run --all-extras dagster dev
```

## Testing

```bash
npm install .. && npm run build '-p ..'
uv run --all-extras pytest
```
# example-project (typescript pipes)

This is an example project showcasing the usage of the typescript dagster-pipes library.

The dagster asset in the orchestrator process `example_typescript_asset`, utilizes 
`PipesSubprocessClient` to run the code of `external_typescript_code` in a pipes context.

## Running

Note that `external_typescript_code` depends on the local (dev) version of `@dagster-io/dagster-pipes`.
You can change the dependency to the latest `npm` release with `npm uninstall @dagster-io/dagster-pipes && npm install @dagster-io/dagster-pipes`

```bash
# Compile the local (dev) version of dagster_pipes_typescript
npm install .. && npm run build '-p ..'
# Run dagster orchestration locally
uv run dagster dev
```


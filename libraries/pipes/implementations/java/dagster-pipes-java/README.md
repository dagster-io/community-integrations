# Dagster Pipes - Java protocol implementation

# Development

## Installation

1. Install the Python (Dagster) environment:

```shell
uv sync
```

This will automatically create a virtual environment in `.venv` and install all the Python dependencies.

To use the environment, either activate it manually with `source ./.venv/bin/activate`, or use `uv run` to execute commands in the context of this environment.

2. Install Java 8. 

To build project, use:
```shell
./gradlew build
```


## Launching an example Dagster project

```shell
uv run dagster dev
```

## Testing

```shell
uv run pytest
```

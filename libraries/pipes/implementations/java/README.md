# Dagster Pipes - Java protocol implementation

This project provides a Java implementation of the Dagster Pipes protocol. It can be used to orchestrate data processing pipelines written in Java from Dagster, while recieving logs and metadata from the Java application.

# Examples

We provide a simple example of how to interact with Dagster from Java. The example is a simple Java application that sends events to Dagster, which can be observer in the Dagster UI.

- [Java code](src/main/java/io/dagster/pipes/utils/Example.java)
- [Dagster code](examples/local/definitions.py)

In order to run the example, follow these steps:

1. Set the `DAGSTER_HOME` environment variable. It should point to any existing directory.
2. Run the Dagster example with [uvx](https://docs.astral.sh/uv/guides/tools/). (alternatively, use the method of preference to install the Python `dagster` package):

```
uvx --with dagster-webserver dagster dev -f examples/local/definitions.py
```

3. Open the Dagster UI at [http://localhost:3000](http://localhost:3000).
4. Materialize the Dagster asset and observe Pipes events produced by the external Java process.

# Development

## Prerequisites

- [uv](https://docs.astral.sh/uv/)
- [Java 8](https://www.oracle.com/java/technologies/javase-jdk8-downloads.html)

For `nix` users, these dependencies can be installed with `nix develop .#java`.

## Installation

1. Install the Python (Dagster) environment, mainly used for testing.

```shell
uv sync
```

This will automatically create a virtual environment in `.venv` and install all the Python dependencies.

To use the environment, either activate it manually with `source ./.venv/bin/activate`, or use `uv run` to execute commands in the context of this environment.

2. To build the Java part of the project, use:
```shell
./gradlew build
```

## Testing

The tests are written in Python and can be run with `pytest`. The Java project will be automatically built before running the tests.

```shell
uv run pytest
```

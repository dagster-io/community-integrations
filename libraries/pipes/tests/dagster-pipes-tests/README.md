# Dagster Pipes Tests

This is a "golden" test suite for the Dagster Pipes protocol. It is designed to test the protocol implementation in various languages, ensuring that the protocol is implemented correctly and consistently across all of them.

# Usage

To make use of this test suite, it has to be installed as a Python package.

`dagster-pipes-tests` is a `pytest` plugin which provides various fixtures and utilities for testing Pipes.

A `pipes.toml` file is required in the root of the project. This file should describe the capabilities of the Pipes implementation being tested. For example, if the implementation supports S3 but not Databricks, the file should look like this:

```toml
[message_channel]
s3 = true
databricks = false
```

See the [pipes_config.py](src/dagster_pipes_tests/pipes_config.py) class for more information on the available configuration options.

In order to run the tests, follow these steps:

1. Install `pytest` and `dagster-pipes-tests`. This can be done with [uv](https://docs.astral.sh/uv/):

```shell
# assuming the command is run in libraries/pipes/implementations/<language>
uv add --group dev pytest --editable ../../tests/dagster-pipes-tests
```

> [!NOTE]
> To install `dagster-pipes-tests` in a repository other than this one, replace `--editable ../../tests/dagster-pipes-tests` with `git+https://github.com/dagster-io/communioty-integrations.git#subdirectory=libraries/pipes/tests/dagster-pipes-tests`

2. Import the test suite in your `pytest` code (for example, in `tests/test_pipes.py`) and configure it with the base arguments (usually containing the testing executable). The executable will be invoked with various arguments, and the test suite will assert certain side effects produced by the executable. Base arguments will be concatenated with additional arguments provided by the test suite.

For example, for Java:

```python
from dagster_pipes_tests import PipesTestSuite


class TestJavaPipes(PipesTestSuite):
    BASE_ARGS = [
        "java",
        "-cp",
        "build/libs/dagster-pipes-java-1.0-SNAPSHOT.jar",
        "pipes.MainTest",
    ]
```

> [!NOTE]
> Each test has it's own `--test-name` argument which can be used to identify the test being run.

> [!WARNING]
> This code must be placed in a file that is discovered by `pytest`, e.g. starts with `test_`.

When working with compiled languages, it's recommended to setup a `pytest` fixture that compiles the executable before running the tests. This way, the executable is only compiled once, and the tests can be run multiple times without recompiling.

For example, for Java, put the following code in `conftest.py`:

```python
import pytest
import subprocess


@pytest.fixture(scope="session", autouse=True)
def built_jar():
    subprocess.run(["./gradlew", "build"], check=True)
```

4. Run the tests with `pytest`:

```shell
uv run pytest
```

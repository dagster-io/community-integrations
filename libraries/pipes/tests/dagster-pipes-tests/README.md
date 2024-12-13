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

1. Install `pytest` and `dagster-pipes-tests`:

```shell
uv pip install pytest
# TODO: publish the package to PyPI
uv pip install <path-to-pipes-tests>
```

2. Import the test suite in your `pytest` code and configure it with the base arguments (usually containing the testing executable). The executable will be invoked with various arguments, and the test suite will assert certain side effects produced by the executable. Base arguments will be concatenated with additional arguments provided by the test suite.

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

3 [Optional]. When working with compiled languages, it's recommended to setup a `pytest` fixture that compiles the executable before running the tests. This way, the executable is only compiled once, and the tests can be run multiple times without recompiling.


For example, for Java, put the following code in `conftest.py`:

```python
import pytest
import subprocess


@pytest.fixture(scope="session", autouse=True)
def built_jar():
    subprocess.run(["./gradlew", "build"], check=True)
```

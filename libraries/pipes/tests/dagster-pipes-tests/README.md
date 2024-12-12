# Dagster Pipes Tests

This is a "golden" test suite for the Dagster Pipes protocol. It is designed to test the protocol implementation in various languages, ensuring that the protocol is implemented correctly and consistently across all of them.

# Usage

To make use of this test suite, it has to be installed as a Python package.

The package is a `pytest` plugin which provides various fixtures and utilities for testing Pipes.

A `pipes.toml` file is required in the root of the project. This file should describe the capabilities of the Pipes implementation being tested. For example, if the implementation supports S3 but not Databricks, the file should look like this:

```toml
s3 = true
databricks = false
```

See the [pipes_config.py](src/dagster_pipes_tests/pipes_config.py) class for more information on the available configuration options.

TODO: Add more information on how to use the test suite.

# dagster-loguru

Capture [Loguru](https://github.com/Delgan/loguru) log messages in Dagster's event log and UI.

Dagster natively captures standard Python logging, including the logger returned by
`dagster.get_dagster_logger()`. This package bridges Loguru into that mechanism:
`capture_loguru_logs()` adds a Loguru sink that forwards each record to that logger,
which Dagster manages during every run. No configuration, no decorators, no
monkeypatching, and existing Loguru sinks are left untouched.

## Installation

```sh
pip install dagster-loguru
```

## Usage

Call `capture_loguru_logs()` once at code location load time:

```python
from dagster import asset, Definitions
from dagster_loguru import capture_loguru_logs
from loguru import logger

capture_loguru_logs()


@asset
def my_asset():
    logger.info("This message appears in the Dagster UI.")


defs = Definitions(assets=[my_asset])
```

Loguru's `SUCCESS` and `TRACE` levels are mapped to `INFO` and `DEBUG` respectively.
For `logger.exception()` calls, the exception information (`exc_info`) is forwarded,
so tracebacks are rendered wherever Dagster renders them for standard logging, such
as the console output and the run's stdout/stderr capture. The returned sink id can
be passed to `loguru.logger.remove()` to stop forwarding.

## Test

```sh
make test
```

## Build

```sh
make build
```

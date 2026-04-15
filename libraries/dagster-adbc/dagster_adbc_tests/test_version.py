from importlib.metadata import version

import dagster_adbc


def test_version() -> None:
    assert version("dagster-adbc") == dagster_adbc.__version__

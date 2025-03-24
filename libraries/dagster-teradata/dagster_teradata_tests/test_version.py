from importlib.metadata import version

import dagster_teradata


def test_version():
    assert version("dagster-teradata") == dagster_teradata.__version__

from importlib.metadata import version

import dagster_datahub


def test_version():
    assert version("dagster-datahub") == dagster_datahub.__version__

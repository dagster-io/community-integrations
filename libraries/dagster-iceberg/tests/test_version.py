from importlib.metadata import version

import dagster_iceberg


def test_version():
    assert version("dagster-iceberg") == dagster_iceberg.__version__

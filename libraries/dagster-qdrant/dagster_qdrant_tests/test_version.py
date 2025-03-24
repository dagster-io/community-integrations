from importlib.metadata import version

import dagster_qdrant


def test_version():
    assert version("dagster-qdrant") == dagster_qdrant.__version__

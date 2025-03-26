from importlib.metadata import version

import dagster_obstore


def test_version():
    assert version("dagster-obstore") == dagster_obstore.__version__

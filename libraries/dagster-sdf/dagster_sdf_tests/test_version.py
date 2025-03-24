from importlib.metadata import version

import dagster_sdf


def test_version():
    assert version("dagster-sdf") == dagster_sdf.__version__

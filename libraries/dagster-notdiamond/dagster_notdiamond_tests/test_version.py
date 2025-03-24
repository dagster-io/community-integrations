from importlib.metadata import version

import dagster_notdiamond


def test_version():
    assert version("dagster-notdiamond") == dagster_notdiamond.__version__

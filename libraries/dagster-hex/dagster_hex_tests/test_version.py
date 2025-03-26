from importlib.metadata import version

import dagster_hex


def test_version():
    assert version("dagster-hex") == dagster_hex.__version__

from importlib.metadata import version

import dagster_modal


def test_version():
    assert version("dagster-modal") == dagster_modal.__version__

from importlib.metadata import version

import dagster_contrib_gcp


def test_version():
    assert version("dagster-contrib-gcp") == dagster_contrib_gcp.__version__

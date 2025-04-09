from importlib.metadata import version

import dagster_evidence


def test_version():
    assert version("dagster-evidence") == dagster_evidence.__version__

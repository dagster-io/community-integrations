from importlib.metadata import version

import dagster_anthropic


def test_version():
    assert version("dagster-anthropic") == dagster_anthropic.__version__

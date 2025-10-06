from importlib.metadata import version

import dagster_vertexai


def test_version():
    assert version("dagster-vertexai") == dagster_vertexai.__version__

from importlib.metadata import version

import dagster_weaviate


def test_version():
    assert version("dagster-weaviate") == dagster_weaviate.__version__

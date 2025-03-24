from importlib.metadata import version

import dagster_chroma


def test_version():
    assert version("dagster-chroma") == dagster_chroma.__version__

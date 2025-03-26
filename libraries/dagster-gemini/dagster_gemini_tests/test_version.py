from importlib.metadata import version

import dagster_gemini


def test_version():
    assert version("dagster-gemini") == dagster_gemini.__version__

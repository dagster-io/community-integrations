from importlib.metadata import version

import dagster_livetennisapi


def test_version():
    assert version("dagster-livetennisapi") == dagster_livetennisapi.__version__

from importlib.metadata import version

import dagster_elasticsearch


def test_version() -> None:
    assert version("dagster-elasticsearch") == dagster_elasticsearch.__version__

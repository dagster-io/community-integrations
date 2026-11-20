from importlib.metadata import version

import dagster_pixeltable


def test_version() -> None:
    assert version("dagster-pixeltable") == dagster_pixeltable.__version__

from dagster._core.libraries import DagsterLibraryRegistry

from dagster_notdiamond.resources import (
    NotDiamondResource as NotDiamondResource,
    with_usage_metadata as with_usage_metadata,
)
from dagster_notdiamond.version import __version__

DagsterLibraryRegistry.register("dagster-notdiamond", __version__)

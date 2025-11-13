import importlib.metadata

from dagster._core.libraries import DagsterLibraryRegistry

from dagster_salesforce.resource import (
    SalesforceField as SalesforceField,
    SalesforceObject as SalesforceObject,
    SalesforceQueryResult as SalesforceQueryResult,
    SalesforceResource as SalesforceResource,
    SalesforceUpdateResult as SalesforceUpdateResult,
)

try:
    __version__ = importlib.metadata.version(__name__)
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.0.0"

DagsterLibraryRegistry.register(
    "dagster-salesforce", __version__, is_dagster_package=False
)

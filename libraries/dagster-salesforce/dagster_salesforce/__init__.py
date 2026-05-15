from dagster._core.libraries import DagsterLibraryRegistry

from dagster_salesforce.resource import (
    SalesforceResource as SalesforceResource,
    SalesforceObject as SalesforceObject,
    SalesforceQueryResult as SalesforceQueryResult,
    SalesforceUpdateResult as SalesforceUpdateResult,
    SalesforceField as SalesforceField,
)

__version__ = "0.0.2"

DagsterLibraryRegistry.register(
    "dagster-salesforce", __version__, is_dagster_package=False
)

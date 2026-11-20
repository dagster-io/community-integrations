from datetime import datetime, timezone

import pytest

from dagster_salesforce import SalesforceField, SalesforceObject

SINCE = datetime(2024, 1, 15, 10, 30, 45, tzinfo=timezone.utc)


def salesforce_field(name: str) -> SalesforceField:
    return SalesforceField(
        name=name,
        label=name,
        type="string",
        length=0,
        precision=0,
        scale=0,
        digits=0,
        custom=name.endswith("__c"),
        nillable=True,
        createable=True,
        updateable=True,
        unique=False,
        auto_number=False,
        external_id=False,
        calculated=False,
        filterable=True,
        sortable=True,
        groupable=True,
        is_compound=False,
    )


def salesforce_object(name: str, *field_names: str) -> SalesforceObject:
    return SalesforceObject.model_construct(
        name=name,
        fields={field_name: salesforce_field(field_name) for field_name in field_names},
    )


@pytest.mark.parametrize(
    ("sobject", "query"),
    [
        (
            salesforce_object("Account", "Id", "Name"),
            "SELECT Id, Name FROM Account",
        ),
        (
            salesforce_object("ns__Invoice__c", "Id", "ns__Total__c"),
            "SELECT Id, ns__Total__c FROM ns__Invoice__c",
        ),
    ],
)
def test_get_query_selects_every_field(sobject: SalesforceObject, query: str):
    assert sobject.get_query() == query


@pytest.mark.parametrize(
    ("incremental_key", "where"),
    [
        ("SystemModstamp", "SystemModstamp >= 2024-01-15T10:30:45Z"),
        ("Owner.LastModifiedDate", "Owner.LastModifiedDate >= 2024-01-15T10:30:45Z"),
    ],
)
def test_get_query_filters_on_the_incremental_key(incremental_key: str, where: str):
    sobject = salesforce_object("Account", "Id")
    query = sobject.get_query(
        incremental_key=incremental_key, incremental_from=SINCE, limit=10
    )
    assert query == f"SELECT Id FROM Account WHERE {where} LIMIT 10"


HOSTILE_NAMES = [
    "Account WHERE Name != null",
    "Name, (SELECT Id FROM Contacts)",
    "Name'",
    "Name--",
    "Name;",
    "Name/*",
    "Owner..Name",
    "Owner.",
]


@pytest.mark.parametrize("name", [*HOSTILE_NAMES, ""])
def test_get_query_rejects_an_object_name_that_is_not_an_api_name(name: str):
    with pytest.raises(ValueError, match="API name"):
        salesforce_object(name, "Id").get_query()


@pytest.mark.parametrize("name", [*HOSTILE_NAMES, ""])
def test_get_query_rejects_a_field_name_that_is_not_an_api_name(name: str):
    with pytest.raises(ValueError, match="API name"):
        salesforce_object("Account", "Id", name).get_query()


@pytest.mark.parametrize("incremental_key", HOSTILE_NAMES)
def test_get_query_rejects_an_incremental_key_that_is_not_an_api_name(
    incremental_key: str,
):
    with pytest.raises(ValueError, match="API name"):
        salesforce_object("Account", "Id").get_query(
            incremental_key=incremental_key, incremental_from=SINCE
        )


@pytest.mark.parametrize("limit", ["10 OFFSET 5", "10", 10.5])
def test_get_query_rejects_a_limit_that_is_not_an_integer(limit):
    with pytest.raises(TypeError):
        salesforce_object("Account", "Id").get_query(limit=limit)

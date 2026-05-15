import json
import dagster as dg

from datetime import datetime, timedelta, timezone
from dagster._record import record
from dagster_shared.dagster_model import DagsterModel
from pydantic import Field, PrivateAttr
from dataclasses import field
from pathlib import Path
from typing import Any

from simple_salesforce.api import Salesforce
from simple_salesforce.bulk2 import (
    ColumnDelimiter,
    LineEnding,
    Operation,
    SFBulk2Type,
)

from dagster_salesforce.credentials import SalesforceCredentials
from dagster_salesforce.utils import datetime_to_soql_str, _wait_for_job

logger = dg.get_dagster_logger(__name__)


@record
class SalesforceQueryResult:
    """
    Query result from Salesforce. Dataclass version of simple_salesforce.bulk2.QueryResult

    :param locator: The locator.
    :type locator: str
    :param number_of_records: The number of records.
    :type number_of_records: int
    :param records: The records.
    :type records: Optional[str]
    :param file: The file.
    :type file: Optional[str]
    """

    locator: str | None
    number_of_records: int
    records: str | None = None
    file: str | None = None


@record
class SalesforceUpdateResult:
    """
    Result of an update operation in Salesforce.

    :param results: List of results from the update operation.
    :type results: List[Dict[str, Any]]
    :param files: List of files generated during the update operation.
    :type files: List[Path]
    :param successfulRecords: Number of records successfully updated.
    :type successfulRecords: int
    :param failedRecords: Number of records that failed to update.
    :type failedRecords: int
    :param unprocessedRecords: Number of records that were not processed.
    :type unprocessedRecords: int
    """

    number_of_jobs: int = 0
    results: list[dict[str, Any]] = field(default_factory=list)
    files: list[Path] = field(default_factory=list)
    successfulRecords: int = 0
    failedRecords: int = 0
    unprocessedRecords: int = 0


@record
class SalesforceField:
    """Metadata for a Salesforce field."""

    name: str
    label: str
    type: str
    length: int
    precision: int
    scale: int
    digits: int
    custom: bool
    nillable: bool
    createable: bool
    updateable: bool
    unique: bool
    auto_number: bool
    external_id: bool
    calculated: bool
    filterable: bool
    sortable: bool
    groupable: bool
    is_compound: bool
    relationship_name: str | None = None
    reference_to: list[str] | None = None
    picklist_values: list[dict[str, Any]] | None = None
    default_value: Any | None = None


class SalesforceObject(DagsterModel):
    """Metadata for a Salesforce object."""

    name: str
    label: str
    label_plural: str
    custom: bool
    createable: bool
    updateable: bool
    deletable: bool
    queryable: bool
    searchable: bool
    retrieveable: bool
    triggerable: bool
    undeletable: bool
    mergeable: bool
    replicateable: bool
    activateable: bool
    fields: dict[str, SalesforceField]
    _salesforce: Salesforce = PrivateAttr()

    @property
    def bulk2(self) -> SFBulk2Type:
        return getattr(self._salesforce.bulk2, self.name)

    def get_query(
        self,
        incremental_key: str | None = None,
        incremental_from: datetime | None = None,
        limit: int | None = None,
    ) -> str:
        """
        Get the SOQL query for the Salesforce object.

        :param incremental_key: The incremental key.
        :type incremental_key: str
        :param incremental_from: The start date.
        :type incremental_from: datetime
        :param limit: The maximum number of records to download.
        :type limit: int
        """

        query = f"SELECT {', '.join(f.name for f in self.fields.values() if not f.is_compound)} FROM {self.name}"
        if incremental_from and incremental_key:
            query += (
                f" WHERE {incremental_key} >= {datetime_to_soql_str(incremental_from)}"
            )
        if limit:
            query += f" LIMIT {limit}"
        return query

    def query_to_csv(
        self,
        query: str,
        output_directory: Path,
        batch_size: int = 20_000,
        wait: int = 2,
    ) -> list[SalesforceQueryResult]:
        """
        Download data from Salesforce object incrementally.

        :param query: The SOQL query.
        :type query: str
        :param batch_size: The maximum number of records to download in a single csv file.
        :type batch_size: int
        :param output_directory: The output directory.
        :type output_directory: Path
        :param wait: The wait time in seconds.
        :type wait: int
        """
        output_directory.mkdir(parents=True, exist_ok=True)

        logger.info(f"Downloading results of query to CSV:\n{query}")
        res = self.bulk2._client.create_job(operation=Operation.query_all, query=query)
        job_id = res["id"]

        logger.info(f"Bulk job ({job_id}) created, waiting for job to complete...")
        _wait_for_job(
            client=self.bulk2._client, job_id=job_id, is_query=True, wait=wait
        )

        logger.info(
            f"Bulk job ({job_id}) completed, downloading results (batch size: {batch_size})..."
        )
        locator = "INIT"
        results = []
        while locator:
            if locator == "INIT":
                locator = ""
            result = SalesforceQueryResult(
                **self.bulk2._client.download_job_data(
                    path=str(output_directory),
                    job_id=job_id,
                    locator=locator,
                    max_records=batch_size,
                )
            )
            locator = result.locator
            results.append(result)
            logger.info("Downloading records...")

        return results

    def create_record(self, data: dict[str, Any]) -> str:
        """Create a single record."""
        logger.info(f"Creating {self.name} record")
        result = getattr(self._salesforce, self.name).create(data)

        if result["success"]:
            return result["id"]
        else:
            raise Exception(
                f"Failed to create record: {result.get('errors', 'Unknown error')}"
            )

    def update_record(self, record_id: str, data: dict[str, Any]):
        """Update a single record."""
        logger.info(f"Updating {self.name} record {record_id}")
        result = getattr(self._salesforce, self.name).update(record_id, data)

        if not isinstance(result, int) or result < 200 or result >= 300:
            raise Exception(f"Failed to update record: {result}")

    def delete_record(self, record_id: str):
        """Delete a single record."""
        logger.info(f"Deleting {self.name} record {record_id}")
        result = getattr(self._salesforce, self.name).delete(record_id)

        if not isinstance(result, int) or not result == 204:
            raise Exception(f"Failed to delete record: {result}")

    def upsert_records(
        self,
        result_output_directory: Path,
        additional_result_metadata: dict[str, Any] | None = {},
        csv_file: str | None = None,
        records: list[dict[str, str]] | None = None,
        external_id_field: str = "Id",
        batch_size: int | None = None,
        column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
        line_ending: LineEnding = LineEnding.LF,
        wait: int = 5,
    ) -> SalesforceUpdateResult:
        """
        Upsert records based on a unique identifier

        :param result_output_directory: The directory where the results will be saved.
        :type result_output_directory: Path
        :param additional_result_metadata: Additional metadata to include in the result rows.
        :type additional_result_metadata: Optional[Dict[str, Any]]
        :param csv_file: Path to the CSV file containing records to upsert.
        :type csv_file: Optional[str]
        :param records: List of records to upsert.
        :type records: Optional[List[Dict[str, str]]]
        :param external_id_field: The field to use as the external ID for upsert operations
        :type external_id_field: str
        :param batch_size: The maximum number of records to process in a single job.
        :type batch_size: Optional[int]
        :param column_delimiter: The delimiter used in the CSV file.
        :type column_delimiter: ColumnDelimiter
        :param line_ending: The line ending used in the CSV file.
        :type line_ending: LineEnding
        :param wait: The time to wait for the job to complete.
        :type wait: int

        :return: A SalesforceUpdateResult containing the results of the upsert operation and a list of files of the resulting records created.
        :rtype: SalesforceUpdateResult
        """

        result_output_directory.mkdir(exist_ok=True, parents=True)
        results = self.bulk2.upsert(
            csv_file=csv_file,
            records=records,
            external_id_field=external_id_field,
            batch_size=batch_size,
            column_delimiter=column_delimiter,
            line_ending=line_ending,
            wait=wait,
        )

        result_data = {
            "number_of_jobs": len(results),
            "results": results,
            "files": [],
            "successfulRecords": 0,
            "failedRecords": 0,
            "unprocessedRecords": 0,
        }

        for result in results:
            for result_type, records in self.bulk2.get_all_ingest_records(
                str(result["job_id"])
            ).items():
                if not records:
                    continue

                result_data[result_type] += len(records)
                record_file = (
                    result_output_directory / f"{result['job_id']}_{result_type}.jsonl"
                )
                with open(record_file, "w") as f:
                    for record in records:
                        json.dump(
                            {
                                "job_id": result["job_id"],
                                "result_type": result_type,
                                **(additional_result_metadata or {}),
                                **record,
                            },
                            f,
                        )
                        f.write("\n")

                result_data["files"].append(record_file)

        return SalesforceUpdateResult(**result_data)


class SalesforceResource(dg.ConfigurableResource):
    """A Dagster resource for Salesforce operations.

    This resource provides comprehensive Salesforce functionality including querying,
    bulk operations, and object manipulation. Supports multiple authentication methods
    through different credential types.

    Note on Bulk Operations:
        This resource uses Salesforce Bulk API 2.0, which automatically handles
        internal batching. When you upload data for bulk operations, Salesforce
        automatically splits it into batches of 10,000 records internally.
        Bulk API 2.0 doesn't support manual batching - all data must be uploaded
        in a single operation (up to 150MB).

    Examples:
        Using Password Authentication:

        .. code-block:: python

            from dagster_salesforce import SalesforceResource, SalesforceUserPasswordCredentials

            salesforce_resource = SalesforceResource(
                credentials=SalesforceUserPasswordCredentials(
                    username="user@example.com",
                    password="your_password",
                    security_token="your_security_token",
                    domain="login"
                )
            )

        Using OAuth 2.0 JWT Bearer Token Authentication:

        .. code-block:: python

            from dagster_salesforce import SalesforceResource, SalesforceJWTOAuthCredentials

            salesforce_resource = SalesforceResource(
                credentials=SalesforceJWTOAuthCredentials(
                    username="user@example.com",
                    consumer_key="your_consumer_key",
                    privatekey_file="/path/to/private.key"
                )
            )

        Using OAuth 2.0 Connected App Authentication:

        .. code-block:: python

            from dagster_salesforce import SalesforceResource, SalesforceConnectedAppOAuthCredentials

            salesforce_resource = SalesforceResource(
                credentials=SalesforceConnectedAppOAuthCredentials(
                    consumer_key="your_consumer_key",
                    consumer_secret="your_consumer_secret"
                )
            )

        Using Direct Session Access:

        .. code-block:: python

            from dagster_salesforce import SalesforceResource, SalesforceSessionCredentials

            salesforce_resource = SalesforceResource(
                credentials=SalesforceSessionCredentials(
                    session_id="your_session_id",
                    instance_url="https://na1.salesforce.com"
                )
            )

        Using the resource to query:

        .. code-block:: python

            # Get an object client
            account_obj = salesforce_resource.get_object_client("Account")

            # Query data
            results = account_obj.query_to_csv(
                query="SELECT Id, Name FROM Account LIMIT 10",
                output_directory=Path("/path/to/output")
            )
    """

    credentials: SalesforceCredentials = Field(
        ...,
        description="Credentials for connecting to Salesforce",
    )

    _sf_client: Salesforce | None = PrivateAttr(default=None)
    _access_token: str | None = PrivateAttr(default=None)
    _instance_url: str | None = PrivateAttr(default=None)
    _token_expiry: datetime | None = PrivateAttr(default=None)

    def _connect(self) -> None:
        # Check if we have a valid token
        if (
            self._access_token
            and self._token_expiry
            and datetime.now(timezone.utc) < self._token_expiry
        ):
            return

        try:
            # Create Salesforce client with JWT
            self._sf_client = Salesforce(**self.credentials.connection_kwargs())

            connection_result = self.check_connection()

            logger.info(
                f"Successfully authenticated to Salesforce: {connection_result}"
            )

            # Store instance URL and access token for potential direct API calls
            self._access_token = self._sf_client.session_id
            self._instance_url = self._sf_client.base_url
            self._token_expiry = datetime.now(timezone.utc) + timedelta(
                hours=2
            )  # Salesforce tokens typically last 2 hours
        except Exception as e:
            raise dg.Failure("Failed to authenticate to Salesforce") from e

    @property
    def client(self) -> Salesforce:
        """Get authenticated Salesforce client."""
        self._connect()

        if self._sf_client is None:
            raise ValueError("Salesforce client is not initialized")

        return self._sf_client

    def check_connection(self) -> dict[str, Any]:
        """Check if authentication is successful and return instance information.

        :return: Dictionary containing connection status and instance information
        :rtype: Dict[str, Any]

        Example response:
            {
                'authenticated': True,
                'instance_url': 'https://yourinstance.salesforce.com',
                'user_id': '00530000009M943',
                'organization_id': '00D300000006GMAAE2',
                'username': 'user@example.com',
                'api_version': '60.0',
                'instance_type': 'Production',
                'instance_name': 'NA123'
            }
        """
        try:
            # Try to get username from credentials (not all credential types have it)
            username = getattr(self.credentials, "username", None)

            if self._sf_client is None:
                raise ValueError("Salesforce client is not initialized")

            # Get user info - if we have a username, use it to query
            if username:
                user_info = self._sf_client.query(
                    f"SELECT Id, Username, Name, Email FROM User WHERE Username = '{username}' LIMIT 1"
                )
                current_user = user_info["records"][0] if user_info["records"] else {}
            else:
                # For session-based auth, we don't have the username upfront
                # Try to get the current user from UserInfo
                current_user = {}

            # Get organization info
            org_info = self._sf_client.query(
                "SELECT Id, Name, OrganizationType, InstanceName, IsSandbox FROM Organization LIMIT 1"
            )
            organization = org_info["records"][0] if org_info["records"] else {}

            return {
                "authenticated": True,
                "instance_url": self._instance_url,
                "user_id": current_user.get("Id", "Unknown"),
                "username": current_user.get("Username", username or "Unknown"),
                "user_name": current_user.get("Name", "Unknown"),
                "user_email": current_user.get("Email", "Unknown"),
                "organization_id": organization.get("Id", "Unknown"),
                "organization_name": organization.get("Name", "Unknown"),
                "organization_type": organization.get("OrganizationType", "Unknown"),
                "is_sandbox": organization.get("IsSandbox", False),
                "api_version": self.credentials.version,
                "instance_type": "Sandbox"
                if organization.get("IsSandbox", False)
                else "Production",
            }
        except Exception as e:
            raise e

    def get_limits(self) -> dict[str, dict[str, int]]:
        """Get API limits and usage information for the Salesforce instance.

        :return: Dictionary containing API limits and current usage
        :rtype: Dict[str, Dict[str, int]]

        Example response:
            {
                'DailyApiRequests': {'Max': 15000, 'Remaining': 14950},
                'DailyBulkApiRequests': {'Max': 10000, 'Remaining': 9999},
                'DailyAsyncApexExecutions': {'Max': 250000, 'Remaining': 249999},
                ...
            }
        """
        return self.client.limits()

    def get_available_objects(self) -> list[str]:
        """Get a list of all available Salesforce objects in the instance.

        :return: List of object API names
        :rtype: List[str]

        Example:
            ['Account', 'Contact', 'Lead', 'Opportunity', ...]
        """
        describe_result = self.client.describe()
        if not describe_result:
            return []

        return [obj["name"] for obj in describe_result["sobjects"] if obj["queryable"]]

    def get_object_client(self, sobject: str) -> SalesforceObject:
        """Get a SalesforceObject client for a specific object. about a Salesforce object.

        :param sobject: Salesforce object API name (e.g., 'Account', 'Contact')
        :type sobject: str
        :return: SalesforceObject containing object metadata
        :rtype: SalesforceObject
        """
        metadata = getattr(self.client, sobject).describe()

        # Convert fields list to dictionary of SalesforceField objects
        fields_dict = {}

        compound_fields = {
            f["compoundFieldName"]
            for f in metadata["fields"]
            if f["compoundFieldName"] is not None
        } - {"Name"}

        for field_data in metadata.get("fields", []):
            field = SalesforceField(
                name=field_data["name"],
                label=field_data.get("label", ""),
                type=field_data.get("type", ""),
                length=field_data.get("length", 0),
                precision=field_data.get("precision", 0),
                scale=field_data.get("scale", 0),
                digits=field_data.get("digits", 0),
                custom=field_data.get("custom", False),
                nillable=field_data.get("nillable", False),
                createable=field_data.get("createable", False),
                updateable=field_data.get("updateable", False),
                unique=field_data.get("unique", False),
                auto_number=field_data.get("autoNumber", False),
                external_id=field_data.get("externalId", False),
                calculated=field_data.get("calculated", False),
                filterable=field_data.get("filterable", False),
                sortable=field_data.get("sortable", False),
                groupable=field_data.get("groupable", False),
                relationship_name=field_data.get("relationshipName"),
                reference_to=field_data.get("referenceTo"),
                picklist_values=field_data.get("picklistValues"),
                default_value=field_data.get("defaultValue"),
                is_compound=field_data["name"] in compound_fields,
            )
            fields_dict[field.name] = field

        obj = SalesforceObject.model_construct(
            name=metadata.get("name", ""),
            label=metadata.get("label", ""),
            label_plural=metadata.get("labelPlural", ""),
            custom=metadata.get("custom", False),
            createable=metadata.get("createable", False),
            updateable=metadata.get("updateable", False),
            deletable=metadata.get("deletable", False),
            queryable=metadata.get("queryable", False),
            searchable=metadata.get("searchable", False),
            retrieveable=metadata.get("retrieveable", False),
            triggerable=metadata.get("triggerable", False),
            undeletable=metadata.get("undeletable", False),
            mergeable=metadata.get("mergeable", False),
            replicateable=metadata.get("replicateable", False),
            activateable=metadata.get("activateable", False),
            fields=fields_dict,
            _salesforce=self.client,
        )
        return obj

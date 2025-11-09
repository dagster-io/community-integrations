# dagster-salesforce

A Dagster integration for [Salesforce](https://www.salesforce.com/) that provides resources for interacting with the Salesforce API, including querying data, managing records, and performing bulk operations.

## Features

- **Multiple authentication methods**: Password, OAuth 2.0, JWT Bearer Token, and Session-based authentication
- **Bulk operations**: Efficiently handle large-scale data operations using Salesforce Bulk API 2.0
- **Query support**: Execute SOQL queries and export results to CSV
- **CRUD operations**: Create, read, update, and delete individual records
- **Upsert support**: Update or insert records based on external IDs
- **Object metadata**: Access Salesforce object schemas and field information
- **Type-safe**: Full type hints and Pydantic models for data validation

## Installation

```bash
uv pip install dagster-salesforce
```

## Example Usage

### Password Authentication

The simplest authentication method using username, password, and security token:

```python
import dagster as dg
from dagster_salesforce import SalesforceResource
from dagster_salesforce.credentials import SalesforceUserPasswordCredentials
from pathlib import Path

@dg.asset
def salesforce_accounts(context: dg.AssetExecutionContext, salesforce: SalesforceResource):
    """Download all Account records from Salesforce."""
    # Get the Account object client
    account_obj = salesforce.get_object_client("Account")

    # Query accounts to CSV
    output_dir = Path("/tmp/salesforce_accounts")
    results = account_obj.query_to_csv(
        query="SELECT Id, Name, Type, Industry FROM Account",
        output_directory=output_dir,
        batch_size=10000
    )

    context.log.info(f"Downloaded {sum(r.number_of_records for r in results)} accounts")
    return {"output_files": [r.file for r in results if r.file]}

defs = dg.Definitions(
    assets=[salesforce_accounts],
    resources={
        "salesforce": SalesforceResource(
            credentials=SalesforceUserPasswordCredentials(
                username=dg.EnvVar("SALESFORCE_USERNAME"),
                password=dg.EnvVar("SALESFORCE_PASSWORD"),
                security_token=dg.EnvVar("SALESFORCE_SECURITY_TOKEN"),
                domain="login"  # or "test" for sandbox
            )
        )
    }
)
```

### JWT Bearer Token Authentication (Recommended for Production)

For production environments, JWT authentication provides secure, token-based access:

```python
import dagster as dg
from dagster_salesforce import SalesforceResource
from dagster_salesforce.credentials import SalesforceJWTOAuthCredentials
from pathlib import Path

@dg.asset
def salesforce_contacts(salesforce: SalesforceResource):
    """Upsert contacts into Salesforce."""
    contact_obj = salesforce.get_object_client("Contact")

    # Prepare records to upsert
    records = [
        {"Email": "john.doe@example.com", "FirstName": "John", "LastName": "Doe"},
        {"Email": "jane.smith@example.com", "FirstName": "Jane", "LastName": "Smith"},
    ]

    # Upsert using email as external ID
    result = contact_obj.upsert_records(
        result_output_directory=Path("/tmp/upsert_results"),
        records=records,
        external_id_field="Email",
        wait=10
    )

    return {
        "successful": result.successfulRecords,
        "failed": result.failedRecords,
        "files": result.files
    }

defs = dg.Definitions(
    assets=[salesforce_contacts],
    resources={
        "salesforce": SalesforceResource(
            credentials=SalesforceJWTOAuthCredentials(
                username=dg.EnvVar("SALESFORCE_USERNAME"),
                consumer_key=dg.EnvVar("SALESFORCE_CONSUMER_KEY"),
                privatekey_file=dg.EnvVar("SALESFORCE_PRIVATE_KEY_PATH")
                # Or use privatekey=dg.EnvVar("SALESFORCE_PRIVATE_KEY") for key content
            )
        )
    }
)
```

### OAuth 2.0 Connected App Authentication

For web applications using OAuth 2.0 flow:

```python
import dagster as dg
from dagster_salesforce import SalesforceResource
from dagster_salesforce.credentials import SalesforceConnectedAppOAuthCredentials

defs = dg.Definitions(
    resources={
        "salesforce": SalesforceResource(
            credentials=SalesforceConnectedAppOAuthCredentials(
                consumer_key=dg.EnvVar("SALESFORCE_CONSUMER_KEY"),
                consumer_secret=dg.EnvVar("SALESFORCE_CONSUMER_SECRET"),
                domain="login"
            )
        )
    }
)
```

### Direct Session Authentication

For using an existing session token:

```python
import dagster as dg
from dagster_salesforce import SalesforceResource
from dagster_salesforce.credentials import SalesforceSessionCredentials

defs = dg.Definitions(
    resources={
        "salesforce": SalesforceResource(
            credentials=SalesforceSessionCredentials(
                session_id=dg.EnvVar("SALESFORCE_SESSION_ID"),
                instance_url="https://your-instance.salesforce.com"
            )
        )
    }
)
```

## Working with Salesforce Objects

### Query Data

```python
import dagster as dg
from dagster_salesforce import SalesforceResource
from datetime import datetime, timedelta
from pathlib import Path

@dg.asset
def query_opportunities(salesforce: SalesforceResource):
    """Query opportunities with filtering."""
    opportunity_obj = salesforce.get_object_client("Opportunity")

    # Build query with date filter
    last_month = datetime.now() - timedelta(days=30)

    query = opportunity_obj.get_query(
        incremental_key="LastModifiedDate",
        incremental_from=last_month,
        limit=1000
    )

    # Execute query and save to CSV
    results = opportunity_obj.query_to_csv(
        query=query,
        output_directory=Path("/tmp/opportunities")
    )

    return results
```

### Create, Update, and Delete Records

```python
import dagster as dg
from dagster_salesforce import SalesforceResource

@dg.asset
def manage_leads(salesforce: SalesforceResource):
    """Demonstrate CRUD operations on Lead records."""
    lead_obj = salesforce.get_object_client("Lead")

    # Create a new lead
    lead_id = lead_obj.create_record({
        "FirstName": "Test",
        "LastName": "Lead",
        "Company": "Test Company",
        "Email": "test@example.com"
    })

    # Update the lead
    lead_obj.update_record(lead_id, {
        "Status": "Qualified",
        "Rating": "Hot"
    })

    # Delete the lead (if needed)
    # lead_obj.delete_record(lead_id)

    return {"created_lead_id": lead_id}
```

### Bulk Operations

```python
import dagster as dg
from dagster_salesforce import SalesforceResource
from pathlib import Path

@dg.asset
def bulk_import_accounts(salesforce: SalesforceResource):
    """Import accounts using Bulk API 2.0."""
    account_obj = salesforce.get_object_client("Account")

    # For large datasets, you can use CSV files
    csv_file = Path("/path/to/accounts.csv")

    result = account_obj.upsert_records(
        result_output_directory=Path("/tmp/bulk_results"),
        csv_file=str(csv_file),
        external_id_field="External_Id__c",
        batch_size=10000,  # Salesforce automatically handles batching
        wait=30
    )

    return {
        "total_processed": result.successfulRecords + result.failedRecords,
        "success_rate": result.successfulRecords / (result.successfulRecords + result.failedRecords)
        if (result.successfulRecords + result.failedRecords) > 0 else 0
    }
```

### Access Field Metadata

```python
import dagster as dg
from dagster_salesforce import SalesforceResource

@dg.asset
def analyze_object_schema(salesforce: SalesforceResource):
    """Analyze Salesforce object schema and fields."""
    # Get Account object metadata
    account_obj = salesforce.get_object_client("Account")

    # Access field information
    field_summary = {}
    for field_name, field in account_obj.fields.items():
        if field.custom:  # Only custom fields
            field_summary[field_name] = {
                "label": field.label,
                "type": field.type,
                "required": not field.nillable,
                "updateable": field.updateable
            }

    return field_summary
```

## Notes

- **Bulk API 2.0**: This integration uses Salesforce Bulk API 2.0, which automatically handles internal batching. When uploading data, Salesforce automatically splits it into batches of 10,000 records.
- **Rate Limits**: The resource includes built-in handling for Salesforce API rate limits. Use `get_limits()` to monitor your API usage.
- **Field Types**: The integration preserves Salesforce field types and handles type conversions appropriately.
- **Error Handling**: Failed records in bulk operations are captured and reported in the result objects.

## API Reference

### SalesforceResource

The main resource for interacting with Salesforce:

- `get_object_client(sobject: str) -> SalesforceObject`: Get a client for a specific Salesforce object
- `get_available_objects() -> List[str]`: List all available Salesforce objects
- `get_limits() -> Dict`: Get API usage limits
- `check_connection() -> Dict`: Verify connection and get instance information

### SalesforceObject

Represents a Salesforce object (table) with methods for data operations:

- `query_to_csv(query: str, output_directory: Path, batch_size: int) -> List[SalesforceQueryResult]`: Execute SOQL query and save results to CSV files
- `create_record(data: Dict) -> str`: Create a single record
- `update_record(record_id: str, data: Dict)`: Update a single record
- `delete_record(record_id: str)`: Delete a single record
- `upsert_records(...)`: Bulk upsert operation
- `get_query(incremental_key: str, incremental_from: datetime, limit: int) -> str`: Build SOQL query with filters

### Credentials Classes

- `SalesforceUserPasswordCredentials`: Username/password authentication
- `SalesforceJWTOAuthCredentials`: JWT Bearer Token authentication
- `SalesforceConnectedAppOAuthCredentials`: OAuth 2.0 Connected App authentication
- `SalesforceSessionCredentials`: Direct session access

## Testing

```bash
# Run tests
make test

# Run linting and formatting
make ruff

# Run type checking
make check
```

## Development

```bash
# Install development dependencies
make install

# Build the package
make build
```

## License

See LICENSE file in the repository.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

# dagster-backblaze

A Dagster integration for [Backblaze B2](https://www.backblaze.com/cloud-storage) object storage.

`B2Resource` is a typed Dagster resource for Backblaze B2's S3-compatible API. It builds on `dagster_aws.s3.S3Resource`, pre-fills the B2 endpoint, and accepts B2-native credential field names so you do not have to remember the endpoint URL or the AWS-named credential fields.

Backblaze B2 speaks the same S3 API as AWS S3, so under the hood `B2Resource` hands you a regular boto3 S3 client pointed at your B2 region. Anything you can do with a boto3 S3 client works against B2.

## Installation

```sh
uv add dagster-backblaze
```

## Usage

```python
from dagster import Definitions, asset
from dagster_backblaze import B2Resource


@asset
def my_asset(b2: B2Resource):
    client = b2.get_client()
    client.put_object(Bucket="my-bucket", Key="hello", Body=b"world")


defs = Definitions(
    assets=[my_asset],
    resources={
        "b2": B2Resource(
            application_key_id="...",
            application_key="...",
        )
    },
)
```

### Configuration

| Field | Description |
| --- | --- |
| `application_key_id` | Backblaze B2 application key ID. Mapped onto the boto3 `aws_access_key_id`. |
| `application_key` | Backblaze B2 application key. Mapped onto the boto3 `aws_secret_access_key`. |
| `endpoint_url` | B2 S3-compatible endpoint. Defaults to `https://s3.us-west-004.backblazeb2.com`. Override to target a different B2 region. |

Credentials and endpoint can also be supplied through environment variables, which the resource reads when the matching field is not set:

- `B2_APPLICATION_KEY_ID`
- `B2_APPLICATION_KEY`
- `B2_ENDPOINT_URL`

Explicit constructor values always take precedence over environment variables. Because `B2Resource` subclasses `S3Resource`, every field on `S3Resource` (such as `region_name`, `use_ssl`, and `max_attempts`) is available as well.

### Targeting a different region

```python
B2Resource(
    application_key_id="...",
    application_key="...",
    endpoint_url="https://s3.eu-central-003.backblazeb2.com",
)
```

### Using AWS S3 or another S3-compatible target

`B2Resource` defaults to B2 but is a thin layer over `S3Resource`. If you already use `dagster-aws`, you can point `dagster_aws.s3.S3Resource` at B2 directly by setting `endpoint_url` to your B2 endpoint. `B2Resource` simply gives you that path with typed B2 config and B2-flavored field names.

## Test

```sh
make test
```

## Build

```sh
make build
```

from typing_extensions import TYPE_CHECKING
import pytest_cases

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client


@pytest_cases.case(tags="default")
def case_default():
    return {"type": "default"}


@pytest_cases.case(tags="s3")
def case_s3(s3_client: "S3Client", s3_bucket: str):
    return {"type": "s3", "bucket": s3_bucket, "client": s3_client}

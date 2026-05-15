from typing import TYPE_CHECKING

import pytest_cases
from dagster._core.pipes.client import PipesMessageReader
from dagster_aws.pipes import PipesS3MessageReader

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client


@pytest_cases.case(tags="default")
def case_message_reader_default():
    return {"type": "default"}


@pytest_cases.case(tags="s3")
def case_message_reader_s3(s3_client: "S3Client", s3_bucket: str):
    return {"type": "s3", "bucket": s3_bucket, "client": s3_client}


@pytest_cases.fixture
@pytest_cases.parametrize_with_cases("params", cases=".")
def message_reader(params) -> PipesMessageReader | None:
    if params["type"] == "default":
        return None
    elif params["type"] == "s3":
        return PipesS3MessageReader(client=params["client"], bucket=params["bucket"])
    else:
        raise ValueError(f"Unknown type: {params['type']}")

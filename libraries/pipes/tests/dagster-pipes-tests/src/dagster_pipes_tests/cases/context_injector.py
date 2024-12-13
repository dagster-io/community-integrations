import pytest_cases
from dagster._core.pipes.client import PipesContextInjector
from dagster._core.pipes.utils import (
    PipesEnvContextInjector,
    PipesTempFileContextInjector,
)
from dagster_aws.pipes import PipesS3ContextInjector
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client


@pytest_cases.case(tags="env")
def case_context_injector_env():
    return {"type": "env"}


@pytest_cases.case(tags="tempfile")
def case_context_injector_tempfile():
    return {"type": "tempfile"}


@pytest_cases.case(tags="s3")
def case_context_injector_s3(s3_client: "S3Client", s3_bucket: str):
    return {"type": "s3", "bucket": s3_bucket, "client": s3_client}


@pytest_cases.fixture
@pytest_cases.parametrize_with_cases("params", cases=".")
def context_injector(params) -> PipesContextInjector:
    if params["type"] == "env":
        return PipesEnvContextInjector()
    elif params["type"] == "tempfile":
        return PipesTempFileContextInjector()
    elif params["type"] == "s3":
        return PipesS3ContextInjector(client=params["client"], bucket=params["bucket"])
    else:
        raise ValueError(f"Unknown type: {params['type']}")

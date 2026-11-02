import os
from datetime import timedelta
from uuid import uuid4

import boto3
import dagster as dg
from botocore.config import Config as BotoConfig
from nebius.aio.cli_config import Config
from nebius.api.nebius.ai.v1 import JobSpec
from nebius.api.nebius.compute.v1 import DiskSpec
from nebius.sdk import SDK

from dagster_nebius import PipesNebiusClient


def sdk_factory():
    # For production use service-account credentials with automatic renewal.
    return SDK(config_reader=Config())


def s3_factory():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["NEBIUS_S3_ENDPOINT"],
        region_name=os.environ["AWS_DEFAULT_REGION"],
        config=BotoConfig(
            connect_timeout=5, read_timeout=10, retries={"max_attempts": 2}
        ),
    )


def job_spec():
    return JobSpec(
        image=os.environ["NEBIUS_JOB_IMAGE"],
        platform=os.environ["NEBIUS_PLATFORM"],
        preset=os.environ["NEBIUS_PRESET"],
        subnet_id=os.environ["NEBIUS_SUBNET_ID"],
        timeout=timedelta(minutes=5),
        restart_attempts=0,
        disk=JobSpec.DiskSpec(
            type=DiskSpec.DiskType.NETWORK_SSD, size_bytes=100 * 1024**3
        ),
        environment_variables=[
            JobSpec.EnvironmentVariable(name=key, value=os.environ[key])
            for key in ["NEBIUS_S3_ENDPOINT", "AWS_DEFAULT_REGION"]
        ]
        + [
            JobSpec.EnvironmentVariable(
                name=key,
                mysterybox_secret=JobSpec.MysteryBoxSecretRef(
                    secret_id=os.environ["NEBIUS_S3_SECRET_ID"]
                ),
            )
            for key in ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]
        ],
    )


@dg.asset
def gpu_result(context: dg.AssetExecutionContext, nebius: PipesNebiusClient):
    return nebius.run(
        context=context,
        job_spec=job_spec(),
        project_id=os.environ["NEBIUS_PROJECT_ID"],
        wait_timeout=600,
        extras={
            "output_bucket": os.environ["NEBIUS_OUTPUT_BUCKET"],
            "output_prefix": f"dagster-results/{context.run.run_id}/{uuid4()}",
        },
    ).get_materialize_result(implicit_materialization=False)


defs = dg.Definitions(
    assets=[gpu_result],
    resources={
        "nebius": PipesNebiusClient(
            sdk_factory=sdk_factory,
            s3_client_factory=s3_factory,
            bucket=os.environ["NEBIUS_PIPES_BUCKET"],
        )
    },
)

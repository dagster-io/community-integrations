"""Small GPU computation; persist the output before reporting materialization."""

import json
import os

import boto3
import torch
from botocore.config import Config
from dagster_pipes import PipesS3MessageWriter, open_dagster_pipes

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["NEBIUS_S3_ENDPOINT"],
    region_name=os.environ["AWS_DEFAULT_REGION"],
    config=Config(connect_timeout=5, read_timeout=10, retries={"max_attempts": 2}),
)
with open_dagster_pipes(message_writer=PipesS3MessageWriter(s3)) as pipes:
    if not torch.cuda.is_available():
        raise RuntimeError("The example requires a CUDA GPU")
    torch.manual_seed(42)
    x = torch.randn(128, 128, device="cuda")
    result = {"mean": (x @ x.T).mean().item(), "gpu": torch.cuda.get_device_name(0)}
    body = json.dumps(result).encode()
    bucket = pipes.get_extra("output_bucket")
    key = pipes.get_extra("output_prefix") + "/result.json"
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
    pipes.log.info("GPU computation complete; output uploaded")
    pipes.report_asset_materialization(
        metadata={"output_uri": f"s3://{bucket}/{key}", "gpu": result["gpu"]},
    )
s3.close()

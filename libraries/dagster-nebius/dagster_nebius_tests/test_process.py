"""Real dagster-pipes worker in a subprocess, using local HTTP S3 (no cloud)."""

import os
import subprocess
import sys

import boto3
from moto.server import ThreadedMotoServer

from dagster_nebius_tests.test_pipes import FakeService, client_for

WORKER = """
import os
import boto3
from dagster_pipes import open_dagster_pipes, PipesS3MessageWriter
client = boto3.client("s3", endpoint_url=os.environ["TEST_S3_ENDPOINT"],
    region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test")
with open_dagster_pipes(message_writer=PipesS3MessageWriter(client, interval=0.01)) as pipes:
    assert pipes.get_extra("input_uri") == "s3://dataset/input"
    pipes.log.info("remote process log")
    print("remote stdout ✓", flush=True)
    pipes.report_asset_materialization(metadata={"count": 7}, data_version="worker-v1")
"""


def test_real_worker_round_trip(monkeypatch, sdk, spec):
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    try:
        host, port = server.get_host_and_port()
        url = f"http://{host}:{port}"
        s3 = boto3.client(
            "s3",
            endpoint_url=url,
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        s3.create_bucket(Bucket="pipes-test")

        class ProcessService(FakeService):
            def create(self, request, **kwargs):
                response = super().create(request, **kwargs)
                env = dict(os.environ)
                env.update(
                    {e.name: e.value for e in request.spec.environment_variables}
                )
                env["TEST_S3_ENDPOINT"] = url
                subprocess.run(
                    [sys.executable, "-c", WORKER],
                    env=env,
                    check=True,
                    timeout=20,
                    capture_output=True,
                )
                return response

        service = ProcessService(s3, messages=[])
        client = client_for(monkeypatch, sdk, s3, service)
        # The real process startup is part of the invocation deadline.
        from dagster import AssetExecutionContext, asset, materialize

        @asset
        def gpu_result(context: AssetExecutionContext):
            return client.run(
                context=context,
                job_spec=spec,
                project_id="project-test",
                wait_timeout=30,
                extras={"input_uri": "s3://dataset/input"},
            ).get_materialize_result(implicit_materialization=False)

        result = materialize([gpu_result])
        assert result.success
        materialization = result.asset_materializations_for_node("gpu_result")[0]
        assert materialization.metadata["count"].value == 7
        assert materialization.tags is not None
        assert materialization.tags["dagster/data_version"] == "worker-v1"
        assert s3.list_objects_v2(Bucket="pipes-test")["KeyCount"] > 0
    finally:
        server.stop()

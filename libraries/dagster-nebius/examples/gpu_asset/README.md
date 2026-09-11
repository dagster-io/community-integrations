# GPU asset example

Runs a small CUDA matrix multiplication in a Job, uploads its JSON result to
Nebius Object Storage, then reports the asset to Dagster. Running it creates
billable resources.

1. Build `Dockerfile` in this directory and push to a registry accessible to the
   Job. Set `NEBIUS_JOB_IMAGE` to the resulting image digest. Configure private
   registry credentials in the Job spec if required by your registry.
2. Configure Nebius SDK/CLI authentication and separate Object Storage access keys
   for the orchestrator (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`). Create a
   worker secret with payload keys of those same names; set `NEBIUS_S3_SECRET_ID`
   to its secret ID.
   Verify it can write to both the protocol and output buckets. The orchestrator
   needs protocol-object read permission.
3. Set `NEBIUS_PROJECT_ID`, `NEBIUS_SUBNET_ID`, `NEBIUS_PLATFORM`,
   `NEBIUS_PRESET`, `NEBIUS_PIPES_BUCKET`, `NEBIUS_OUTPUT_BUCKET`,
   `NEBIUS_S3_ENDPOINT` and `AWS_DEFAULT_REGION`. The chosen image/platform must
   support CUDA; confirm quota/capacity for your test project.
4. From the package directory, run:

   ```sh
   uv run dagster asset materialize -f examples/gpu_asset/definitions.py --select gpu_result
   ```

Expected result: one materialization with a real output URI and GPU name, plus
Job identity and timing metadata. Retrieve the result directly with an
S3-compatible client configured for Nebius Object Storage; output files are not
downloaded by the integration.

To test cancellation, stop the Dagster run while the Job is active and verify
its final state using the logged Job ID. For a longer-running cancellation
fixture use a dedicated test image with an interruptible sleep, retaining a
finite provider timeout. Preserve evidence before deleting test-owned Job
records or objects. Never delete the shared protocol bucket to clean one run.

The SDK and Boto3 clients are constructed in worker execution. The CUDA image
contains only the workload, dagster-pipes and Boto3. Neither IAM control-plane
credentials nor the Dagster package are passed to the Job.

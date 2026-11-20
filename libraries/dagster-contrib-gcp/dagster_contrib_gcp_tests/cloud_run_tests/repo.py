import dagster

from dagster_contrib_gcp.cloud_run.executor import cloud_run_job_executor


@dagster.op
def node(_):
    pass


@dagster.job
def job():
    node()


NUM_BATCHES = 4


@dagster.op(out=dagster.DynamicOut(int))
def split(_):
    for i in range(NUM_BATCHES):
        yield dagster.DynamicOutput(i, mapping_key=str(i))


@dagster.op
def process_batch(_, batch_id: int) -> int:
    return batch_id


@dagster.job(executor_def=cloud_run_job_executor)
def dynamic_job():
    split().map(process_batch)


@dagster.repository
def repository():
    return [job, dynamic_job]

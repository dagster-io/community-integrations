"""TDLoad demonstration.

TDLoad is a standalone job driven by tdload_operator - it isn't tied to the
I/O manager, so it's modelled as an @op/@job pair here rather than an asset.

Requires the ``tdload`` (Teradata Parallel Transporter) client utilities to be
installed and on PATH. Configure the job vars file / options for your
environment before running.
"""

from dagster import job, op


@op(required_resource_keys={"teradata"})
def run_tdload(context) -> None:
    """Runs a TDLoad job using a job-variables file plus extra CLI options.

    Swap the arguments below for a real ``source_table``/``target_table`` (or
    ``select_stmt``/``insert_stmt``) pair, or a job-variables file, matching
    your own TDLoad job definition.
    """
    return_code = context.resources.teradata.tdload_operator(
        source_table="staging_db.orders_staging",
        target_table="analytics_db.orders",
        tdload_options="-j my_load_job",
    )
    context.log.info(f"tdload exited with return code: {return_code}")


@job
def tdload_job() -> None:
    run_tdload()

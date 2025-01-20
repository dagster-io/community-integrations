import os

import pytest
from dagster import job, op
from dagster_teradata import teradata_resource


@pytest.mark.integration
def test_resume_teradata_compute_cluster(tmp_path):
    @op(required_resource_keys={"teradata"})
    def example_resume_teradata_compute_cluster(context):
        """Args for resume_teradata_compute_cluster():
        compute_profile_name: Name of the Compute Profile to manage.
        compute_group_name: Name of compute group to which compute profile belongs.
        query_strategy: Query strategy to use. Refers to the approach or method used by the
                Teradata Optimizer to execute SQL queries efficiently within a Teradata computer cluster.
                Valid query_strategy value is either 'STANDARD' or 'ANALYTIC'. Default at database level is STANDARD
        compute_map: ComputeMapName of the compute map. The compute_map in a compute cluster profile refers
                to the mapping of compute resources to a specific node or set of nodes within the cluster.
        compute_attribute: Optional attributes of compute profile. Example compute attribute
                MIN_COMPUTE_COUNT(1) MAX_COMPUTE_COUNT(5) INITIALLY_resumeED('FALSE')
                   compute_attribute (str, optional): Additional attributes for compute profile. Defaults to None.
        """
        context.resources.teradata.resume_teradata_compute_cluster("ShippingCG01", "Shipping")

    @job(resource_defs={"teradata": teradata_resource})
    def example_job():
        example_resume_teradata_compute_cluster()

    example_job.execute_in_process(
        run_config={
            'resources': {
                'teradata': {
                    'config': {
                        'host': os.getenv("TERADATA_HOST"),
                        'user': os.getenv("TERADATA_USER"),
                        'password': os.getenv("TERADATA_PASSWORD"),
                        'database': os.getenv("TERADATA_DATABASE"),
                    }
                }
            }
        }
    )

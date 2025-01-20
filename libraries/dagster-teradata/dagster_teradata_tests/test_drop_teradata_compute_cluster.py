import os

import pytest
from dagster import job, op
from dagster_teradata import teradata_resource


@pytest.mark.integration
def test_example_drop_teradata_compute_cluster(tmp_path):
    @op(required_resource_keys={"teradata"})
    def example_drop_teradata_compute_cluster(context):
        """Args for drop_teradata_compute_cluster():
        compute_profile_name: Name of the Compute Profile to manage.
        compute_group_name: Name of compute group to which compute profile belongs.
        delete_compute_group: Indicates whether the compute group should be deleted.
            When set to True, it signals the system to remove the specified compute group.
            Conversely, when set to False, no action is taken on the compute group.
        """
        context.resources.teradata.drop_teradata_compute_cluster("ShippingCG01", "Shipping", True)

    @job(resource_defs={"teradata": teradata_resource})
    def example_job():
        example_drop_teradata_compute_cluster()

    example_job.execute_in_process(
        run_config={
            'resources': {
                'teradata': {
                    'config': {
                        'host' : os.getenv("TERADATA_HOST"),
                        'user' : os.getenv("TERADATA_USER"),
                        'password' : os.getenv("TERADATA_PASSWORD"),
                        'database' : os.getenv("TERADATA_DATABASE"),
                    }
                }
            }
        }
    )


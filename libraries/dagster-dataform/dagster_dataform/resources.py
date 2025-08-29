import dagster as dg
from typing import Optional, List, Dict, Any

from dagster_dataform.utils import get_epoch_time_ago, empty_fn

from google.cloud import dataform_v1


class DataformRepositoryResource:
    """This resource exposes methods for interacting with the Dataform resource via the GCP Python SDK."""

    def __init__(
        self,
        project_id: str,
        repository_id: str,
        location: str,
        environment: str,
        sensor_minimum_interval_seconds: int = 120,
        client: Optional[dataform_v1.DataformClient] = None,
        asset_fresh_policy_lag_minutes: float = 1440,
    ):
        self.project_id = project_id
        self.location = location
        self.repository_id = repository_id
        self.environment = environment
        self.client = client if client is not None else dataform_v1.DataformClient()
        self.logger = dg.get_dagster_logger()
        self.sensor_minimum_interval_seconds = sensor_minimum_interval_seconds
        self.asset_fresh_policy_lag_minutes = asset_fresh_policy_lag_minutes

        self.assets = self.load_dataform_assets(
            fresh_policy_lag_minutes=self.asset_fresh_policy_lag_minutes
        )
        self.asset_checks = self.load_dataform_asset_check_specs()

    def create_compilation_result(
        self,
        git_commitish: str,
        default_database: Optional[str] = None,
        default_schema: Optional[str] = None,
        default_location: Optional[str] = None,
        assertion_schema: Optional[str] = None,
        database_suffix: Optional[str] = None,
        schema_suffix: Optional[str] = None,
        table_prefix: Optional[str] = None,
        builtin_assertion_name_prefix: Optional[str] = None,
        vars: Optional[Dict[str, Any]] = None,
    ) -> dataform_v1.CompilationResult:
        """Create a compilation result and return the name."""
        compilation_result = dataform_v1.CompilationResult()
        compilation_result.git_commitish = git_commitish
        compilation_result.code_compilation_config = dataform_v1.CodeCompilationConfig(
            default_database=default_database,
            default_schema=default_schema,
            default_location=default_location,
            assertion_schema=assertion_schema,
            database_suffix=database_suffix,
            schema_suffix=schema_suffix,
            table_prefix=table_prefix,
            builtin_assertion_name_prefix=builtin_assertion_name_prefix,
            vars=vars,
        )

        request = dataform_v1.CreateCompilationResultRequest(
            parent=f"projects/{self.project_id}/locations/{self.location}/repositories/{self.repository_id}",
            compilation_result=compilation_result,
        )

        response = self.client.create_compilation_result(request=request)

        self.logger.info(f"Created compilation result: {response.name}")

        return response

    def get_latest_compilation_result_name(self) -> Optional[str]:
        """Get the latest compilation result for the repository.
        https://cloud.google.com/python/docs/reference/dataform/latest/google.cloud.dataform_v1.types.ListCompilationResultsRequest
        """

        self.logger.info(
            f"Fetching compilation results for repository: {self.repository_id}"
        )

        request = dataform_v1.ListCompilationResultsRequest(
            parent=f"projects/{self.project_id}/locations/{self.location}/repositories/{self.repository_id}",
            page_size=1000,
            order_by="create_time desc",
        )

        response = self.client.list_compilation_results(request=request)

        self.logger.info(
            f"Found {len(response.compilation_results)} compilation results"
        )

        for compilation_result in response.compilation_results:
            if (
                compilation_result.git_commitish == self.environment
                and not compilation_result.code_compilation_config.table_prefix
            ):
                return compilation_result.name

        self.logger.error(
            f"No compilation result for {self.environment} branch in the last 10 compilation results"
        )
        return None

    def query_compilation_result(self) -> List[Any]:
        """Query a compilation result by ID. Returns the compilation result actions."""

        compilation_result_name = self.get_latest_compilation_result_name()
        if not compilation_result_name:
            self.logger.error("No compilation result name available")
            return []

        self.logger.info(f"Querying compilation result: {compilation_result_name}")

        # Initialize request argument(s)
        request = dataform_v1.QueryCompilationResultActionsRequest(
            name=compilation_result_name,
        )

        # Make the request
        response = self.client.query_compilation_result_actions(request=request)

        self.logger.info(
            f"Found {len(response.compilation_result_actions)} compilation result actions"
        )

        # Handle the response
        return response.compilation_result_actions

    # def create_workflow_invocation(self, repository_id: str, workflow_id: str) -> dict:
    #     """Create a workflow invocation."""
    #     pass

    def get_latest_workflow_invocations(
        self, minutes_ago: int
    ) -> dataform_v1.ListWorkflowInvocationsResponse:
        """Get the latest workflow invocation."""
        request = dataform_v1.ListWorkflowInvocationsRequest(
            parent=f"projects/{self.project_id}/locations/{self.location}/repositories/{self.repository_id}",
            page_size=1000,
            filter=f"invocation_timing.start_time.seconds > {get_epoch_time_ago(minutes=minutes_ago)}",
        )

        response = self.client.list_workflow_invocations(request=request)

        self.logger.info(f"Found response: {response}")

        return response  # pyright: ignore[reportReturnType]

    def query_workflow_invocation(
        self, name: str
    ) -> dataform_v1.QueryWorkflowInvocationActionsResponse:
        """Query a workflow invocation by name."""

        if not name:
            self.logger.error("No workflow invocation name available")
            return []  # pyright: ignore[reportReturnType]

        self.logger.info(f"Querying workflow invocation: {name}")

        # Initialize request argument(s)
        request = dataform_v1.QueryWorkflowInvocationActionsRequest(
            name=name,
        )

        # Make the request
        response = self.client.query_workflow_invocation_actions(request=request)

        # self.logger.info(f"Found {len(response)} workflow invocation actions")

        # Handle the response
        return response  # pyright: ignore[reportReturnType]

    def create_workflow_invocation(
        self, compilation_result_name: str
    ) -> dataform_v1.WorkflowInvocation:
        """Create a workflow invocation. Returns the workflow invocation object."""

        request = dataform_v1.CreateWorkflowInvocationRequest(
            parent=f"projects/{self.project_id}/locations/{self.location}/repositories/{self.repository_id}",
            workflow_invocation=dataform_v1.WorkflowInvocation(
                compilation_result=compilation_result_name,
            ),
        )

        response = self.client.create_workflow_invocation(request=request)

        self.logger.info(f"Created workflow invocation: {response.name}")

        return response

    def get_workflow_invocation_details(
        self, workflow_invocation_name: str
    ) -> dataform_v1.WorkflowInvocation:
        """Get the details of a workflow invocation. Returns the workflow invocation object."""

        request = dataform_v1.GetWorkflowInvocationRequest(
            name=workflow_invocation_name,
        )

        response = self.client.get_workflow_invocation(request=request)

        return response

    def load_dataform_assets(
        self,
        fresh_policy_lag_minutes: float = 1440,
    ) -> List[dg.AssetSpec]:
        logger = dg.get_dagster_logger()
        logger.info("Starting to load Dataform assets")

        assets = []
        self.create_compilation_result(git_commitish=self.environment)
        compilation_actions = self.query_compilation_result()

        logger.info(f"Processing {len(compilation_actions)} compilation actions")

        for asset in compilation_actions:
            try:
                spec = dg.AssetSpec(
                    key=asset.target.name,
                    kinds={"bigquery"},
                    metadata={
                        "Project ID": asset.target.database,
                        "Dataset": asset.target.schema,
                        "Asset Name": asset.target.name,
                        "Docs Link": dg.MetadataValue.url(
                            f"https://cvsdigital.atlassian.net/wiki/spaces/EDMLABCCM/pages/4616946342/Case+Activities+Entity+Data+Stream#{asset.target.name}"
                        ),
                        # "github link": MetadataValue.url(f"https://github.com/cvs-health-source-code/hcm-cm-de-clinical-analytics-nexus-dataform/blob/{client.environment}/definitions/{asset.target.schema.split('_')[4]}/{asset.target.name}.sqlx")
                        "Asset SQL Code": dg.MetadataValue.md(
                            f"```sql\n{asset.relation.select_query}\n```"
                        ),
                    },
                    group_name=asset.target.schema,
                    tags={tag: "" for tag in asset.relation.tags},
                    deps=[target.name for target in asset.relation.dependency_targets],
                    legacy_freshness_policy=dg.LegacyFreshnessPolicy(
                        maximum_lag_minutes=fresh_policy_lag_minutes
                    ),
                )
                assets.append(spec)
                logger.debug(f"Created asset spec for: {asset.target.name}")
            except Exception as e:
                logger.error(
                    f"Failed to create asset spec for {asset.target.name}: {str(e)}"
                )

        logger.info(f"Successfully created {len(assets)} assets")
        return assets

    def load_dataform_asset_check_specs(
        self,
    ) -> List[dg.AssetChecksDefinition]:
        logger = dg.get_dagster_logger()
        logger.info("Starting to load Dataform asset check specs")

        asset_checks = []
        self.create_compilation_result(git_commitish=self.environment)
        compilation_actions = self.query_compilation_result()

        logger.info(f"Processing {len(compilation_actions)} compilation actions")

        for asset in compilation_actions:
            if asset.assertion:
                try:
                    logger.error(asset.assertion.dependency_targets[0].name)
                    asset_key = asset.assertion.parent_action.name

                    # Convert string to AssetKey
                    asset_key_obj = dg.AssetKey(asset_key)

                    spec = dg.AssetCheckSpec(
                        asset=asset_key_obj,  # Use AssetKey object, not string
                        name=asset.target.name,
                    )

                    definition = dg.AssetChecksDefinition.create(
                        keys_by_input_name={
                            "asset_key": asset_key_obj
                        },  # Use AssetKey object
                        node_def=dg.OpDefinition(
                            name=asset.target.name,
                            compute_fn=empty_fn,  # We want to simply define the asset check specifications, not a computations for the check. These checks will not be computed on the Dagster side.
                        ),
                        check_specs_by_output_name={"spec": spec},
                        can_subset=False,
                    )

                    asset_checks.append(definition)
                    logger.debug(f"Created asset check spec for: {asset.target.name}")
                except Exception as e:
                    logger.error(
                        f"Failed to create asset check spec for {asset.target.name}: {str(e)}"
                    )

        logger.error(f"Successfully created {len(asset_checks)} asset check specs")
        return asset_checks

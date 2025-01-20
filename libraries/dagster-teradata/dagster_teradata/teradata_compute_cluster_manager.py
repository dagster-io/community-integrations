import asyncio
import re
import time
from textwrap import dedent

import teradatasql
from dagster import DagsterError

from dagster_teradata import constants


class TeradataComputeClusterManager:
    def __init__(self, connection, log):
        self.connection = connection
        self.log = log
        self.execute_query = connection.execute_query

    def verify_compute_cluster(self, compute_profile_name: str):
        if (
            compute_profile_name is None
            or compute_profile_name == "None"
            or compute_profile_name == ""
        ):
            self.log.info("Invalid compute cluster profile name")
            raise DagsterError(constants.CC_OPR_EMPTY_PROFILE_ERROR_MSG)

        # Getting teradata db version. Considering teradata instance is Lake when db version is 20 or above
        db_version_get_sql = (
            "SELECT  InfoData AS Version FROM DBC.DBCInfoV WHERE InfoKey = 'VERSION'"
        )
        try:
            db_version_result = self.execute_query(db_version_get_sql, True, True)
            if db_version_result is not None:
                db_version_result = str(db_version_result)
                db_version = db_version_result.split(".")[0]
                if db_version is not None and int(db_version) < 20:
                    raise DagsterError(constants.CC_GRP_LAKE_SUPPORT_ONLY_MSG)
            else:
                raise Exception("Error occurred while getting teradata database version")
        except teradatasql.DatabaseError as ex:
            self.log.error("Error occurred while getting teradata database version: %s ", str(ex))
            raise Exception("Error occurred while getting teradata database version")

        lake_support_find_sql = (
            "SELECT count(1) from DBC.StorageV WHERE StorageName='TD_OFSSTORAGE'"
        )
        lake_support_result = self.execute_query(lake_support_find_sql, True, True)
        if lake_support_result is None:
            raise DagsterError(constants.CC_GRP_LAKE_SUPPORT_ONLY_MSG)
        pass

    def handle_cc_status(
        self, operation, sql, compute_profile_name, compute_group_name, timeout
    ) -> str:
        sql_result = self.execute_query(sql, True, True)

        cluster_sync = TeradataComputeClusterSync(
            self.connection,
            self.log,
            operation,
            compute_profile_name,
            compute_group_name,
            constants.CC_POLL_INTERVAL,
            timeout,
        )

        loop = asyncio.get_event_loop()
        loop.run_until_complete(cluster_sync.run())

        sql_result = "%s query ran successfully." % operation
        self.log.info(sql_result)

        return sql_result

    def get_initially_suspended(self, create_cp_query):
        initially_suspended = "FALSE"
        pattern = r"INITIALLY_SUSPENDED\s*\(\s*'(TRUE|FALSE)'\s*\)"
        # Search for the pattern in the input string
        match = re.search(pattern, create_cp_query, re.IGNORECASE)
        if match:
            # Get the value of INITIALLY_SUSPENDED
            initially_suspended = match.group(1).strip().upper()
        return initially_suspended
        pass

    def create_teradata_compute_cluster(
        self,
        compute_profile_name: str,
        compute_group_name: str,
        query_strategy: str = "STANDARD",
        compute_map: str = None,
        compute_attribute: str = None,
        timeout: int = constants.CC_OPR_TIME_OUT,
    ):
        self.verify_compute_cluster(compute_profile_name)

        if compute_group_name:
            # Step 1: Check if the compute group exists
            check_compute_group_sql = dedent(f"""
                        SELECT count(1) FROM DBC.ComputeGroups 
                        WHERE UPPER(ComputeGroupName) = UPPER('{compute_group_name}')
                    """)
            cg_status_result = self.execute_query(check_compute_group_sql, True, True)
            if cg_status_result is not None:
                cg_status_result = str(cg_status_result)
            else:
                cg_status_result = 0

            # Step 2: Create the compute group if it doesn't exist
            if int(cg_status_result) == 0:
                create_cg_query = "CREATE COMPUTE GROUP " + compute_group_name
                if query_strategy is not None:
                    create_cg_query = (
                        create_cg_query + " USING QUERY_STRATEGY ('" + query_strategy + "')"
                    )
                self.execute_query(create_cg_query)

        # Step 3: Check if the compute profile exists within the compute group
        cp_status_query = (
            "SEL ComputeProfileState FROM DBC.ComputeProfilesVX WHERE UPPER(ComputeProfileName) = UPPER('"
            + compute_profile_name
            + "')"
        )
        if compute_group_name:
            cp_status_query += " AND UPPER(ComputeGroupName) = UPPER('" + compute_group_name + "')"
        cp_status_result = self.execute_query(cp_status_query, True, True)
        if cp_status_result is not None:
            cp_status_result = str(cp_status_result)
            msg = f"Compute Profile {compute_profile_name} is already exists under Compute Group {compute_group_name}. Status is {cp_status_result}"
            self.log.info(msg)
            return cp_status_result
        else:
            create_cp_query = "CREATE COMPUTE PROFILE " + compute_profile_name
            if compute_group_name:
                create_cp_query = create_cp_query + " IN " + compute_group_name
            if compute_map is not None:
                create_cp_query = create_cp_query + ", INSTANCE = " + compute_map
            if query_strategy is not None:
                create_cp_query = create_cp_query + ", INSTANCE TYPE = " + query_strategy
            if compute_attribute is not None:
                create_cp_query = create_cp_query + " USING " + compute_attribute
            operation = constants.CC_CREATE_OPR
            initially_suspended = self.get_initially_suspended(create_cp_query)
            if initially_suspended == "TRUE":
                operation = constants.CC_CREATE_SUSPEND_OPR
            return self.handle_cc_status(
                operation, create_cp_query, compute_profile_name, compute_group_name, timeout
            )

    def drop_teradata_compute_cluster(
        self,
        compute_profile_name: str,
        compute_group_name: str,
        delete_compute_group: bool = False,
    ):
        self.verify_compute_cluster(compute_profile_name)

        cp_drop_query = "DROP COMPUTE PROFILE " + compute_profile_name
        if compute_group_name:
            cp_drop_query = cp_drop_query + " IN COMPUTE GROUP " + compute_group_name
        self.execute_query(cp_drop_query)
        self.log.info(
            "Compute Profile %s IN Compute Group %s is successfully dropped",
            compute_profile_name,
            compute_group_name,
        )
        if delete_compute_group:
            cg_drop_query = "DROP COMPUTE GROUP " + compute_group_name
            self.execute_query(cg_drop_query)
            self.log.info("Compute Group %s is successfully dropped", compute_group_name)
        pass

    def resume_teradata_compute_cluster(
        self,
        compute_profile_name: str,
        compute_group_name: str,
        timeout: int = constants.CC_OPR_TIME_OUT,
    ):
        self.verify_compute_cluster(compute_profile_name)

        cc_status_query = (
            "SEL ComputeProfileState FROM DBC.ComputeProfilesVX WHERE UPPER(ComputeProfileName) = UPPER('"
            + compute_profile_name
            + "')"
        )
        if compute_group_name:
            cc_status_query += " AND UPPER(ComputeGroupName) = UPPER('" + compute_group_name + "')"
        cc_status_result = self.execute_query(cc_status_query, True, True)
        if cc_status_result is not None:
            cp_status_result = str(cc_status_result)
        # Generates an error message if the compute cluster does not exist for the specified
        # compute profile and compute group.
        else:
            self.log.info(constants.CC_GRP_PRP_NON_EXISTS_MSG)
            raise DagsterError(constants.CC_GRP_PRP_NON_EXISTS_MSG)
        if cp_status_result != constants.CC_RESUME_DB_STATUS:
            cp_resume_query = f"RESUME COMPUTE FOR COMPUTE PROFILE {compute_profile_name}"
            if compute_group_name:
                cp_resume_query = f"{cp_resume_query} IN COMPUTE GROUP {compute_group_name}"
            return self.handle_cc_status(
                constants.CC_RESUME_OPR,
                cp_resume_query,
                compute_profile_name,
                compute_group_name,
                timeout,
            )
        else:
            self.log.info(
                "Compute Cluster %s already %s", compute_profile_name, constants.CC_RESUME_DB_STATUS
            )

    def suspend_teradata_compute_cluster(
        self,
        compute_profile_name: str,
        compute_group_name: str,
        timeout: int = constants.CC_OPR_TIME_OUT,
    ):
        self.verify_compute_cluster(compute_profile_name)

        sql = (
            "SEL ComputeProfileState FROM DBC.ComputeProfilesVX WHERE UPPER(ComputeProfileName) = UPPER('"
            + compute_profile_name
            + "')"
        )
        if compute_group_name:
            sql += " AND UPPER(ComputeGroupName) = UPPER('" + compute_group_name + "')"
        result = self.execute_query(sql, True, True)
        if result is not None:
            result = str(result)
        # Generates an error message if the compute cluster does not exist for the specified
        # compute profile and compute group.
        else:
            self.log.info(constants.CC_GRP_PRP_NON_EXISTS_MSG)
            raise DagsterError(constants.CC_GRP_PRP_NON_EXISTS_MSG)
        if result != constants.CC_SUSPEND_DB_STATUS:
            sql = f"SUSPEND COMPUTE FOR COMPUTE PROFILE {compute_profile_name}"
            if compute_group_name:
                sql = f"{sql} IN COMPUTE GROUP {compute_group_name}"
            return self.handle_cc_status(
                constants.CC_SUSPEND_OPR, sql, compute_profile_name, compute_group_name, timeout
            )
        else:
            self.log.info(
                "Compute Cluster %s already %s",
                compute_profile_name,
                constants.CC_SUSPEND_DB_STATUS,
            )
        pass


class TeradataComputeClusterSync:
    def __init__(
        self,
        connection,
        log,
        operation: str,
        compute_profile_name: str = None,
        compute_group_name: str = None,
        poll_interval: float | None = None,
        timeout: int = constants.CC_OPR_TIME_OUT,
    ):
        self.connection = connection
        self.operation = operation
        self.compute_profile_name = compute_profile_name
        self.compute_group_name = compute_group_name
        self.poll_interval = poll_interval
        self.timeout = timeout
        self.log = log

    def run(self) -> dict:
        """Wait for Compute Cluster operation to complete."""
        start_time = time.time()  # Record the start time
        try:
            while True:
                # Check for timeout
                elapsed_time = time.time() - start_time
                if elapsed_time > self.timeout:
                    self.log.error(constants.CC_OPR_TIMEOUT_ERROR, self.operation)
                    raise asyncio.CancelledError(
                        f"Operation timed out after {self.timeout} seconds."
                    )
                status = self.get_status()
                if status is None or len(status) == 0:
                    self.log.info(constants.CC_GRP_PRP_NON_EXISTS_MSG)
                    raise DagsterError(constants.CC_GRP_PRP_NON_EXISTS_MSG)
                if (
                    self.operation == constants.CC_SUSPEND_OPR
                    or self.operation == constants.CC_CREATE_SUSPEND_OPR
                ):
                    if status == constants.CC_SUSPEND_DB_STATUS:
                        break
                elif (
                    self.operation == constants.CC_RESUME_OPR
                    or self.operation == constants.CC_CREATE_OPR
                ):
                    if status == constants.CC_RESUME_DB_STATUS:
                        break
                if self.poll_interval is not None:
                    self.poll_interval = float(self.poll_interval)
                else:
                    self.poll_interval = float(constants.CC_POLL_INTERVAL)
                time.sleep(self.poll_interval)
            if (
                self.operation == constants.CC_SUSPEND_OPR
                or self.operation == constants.CC_CREATE_SUSPEND_OPR
            ):
                if status == constants.CC_SUSPEND_DB_STATUS:
                    return constants.CC_OPR_SUCCESS_STATUS_MSG % (
                        self.compute_profile_name,
                        self.operation,
                    )
                else:
                    return constants.CC_OPR_FAILURE_STATUS_MSG % (
                        self.compute_profile_name,
                        self.operation,
                    )
            elif (
                self.operation == constants.CC_RESUME_OPR
                or self.operation == constants.CC_CREATE_OPR
            ):
                if status == constants.CC_RESUME_DB_STATUS:
                    return constants.CC_OPR_SUCCESS_STATUS_MSG % (
                        self.compute_profile_name,
                        self.operation,
                    )
                else:
                    return constants.CC_OPR_FAILURE_STATUS_MSG % (
                        self.compute_profile_name,
                        self.operation,
                    )
            else:
                yield DagsterError("Invalid operation")
        except DagsterError as e:
            yield DagsterError(str(e))
        except asyncio.CancelledError:
            self.log.error(constants.CC_OPR_TIMEOUT_ERROR, self.operation)
            yield DagsterError(constants.CC_OPR_TIMEOUT_ERROR, self.operation)

    def get_status(self) -> str:
        """Return compute cluster SUSPEND/RESUME operation status."""
        sql = f"SELECT ComputeProfileState FROM DBC.ComputeProfilesVX WHERE UPPER(ComputeProfileName) = UPPER('{self.compute_profile_name}')"
        if self.compute_group_name:
            sql += f" AND UPPER(ComputeGroupName) = UPPER('{self.compute_group_name}')"

        if self.compute_group_name:
            sql += " AND UPPER(ComputeGroupName) = UPPER('" + self.compute_group_name + "')"
        result_set = self.connection.execute_query(sql, True, True)
        status = result_set
        if isinstance(result_set, list) and isinstance(result_set[0], str):
            status = str(result_set[0])
        return status

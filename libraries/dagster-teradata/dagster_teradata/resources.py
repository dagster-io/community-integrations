from contextlib import closing, contextmanager
from datetime import datetime
from textwrap import dedent
from typing import Any, List, Mapping, Optional, Sequence, Union, TYPE_CHECKING, Literal

import re
import dagster._check as check
import teradatasql
from dagster import (
    ConfigurableResource,
    IAttachDifferentObjectToOpContext,
    get_dagster_logger,
    resource,
)
from dagster._annotations import public
from dagster._core.definitions.resource_definition import dagster_maintained_resource
from dagster._utils.cached_method import cached_method
from pydantic import Field

from . import constants
from dagster_teradata.ttu.bteq import Bteq
from dagster_teradata.teradata_compute_cluster_manager import (
    TeradataComputeClusterManager,
)


def _handle_user_query_band_text(self, query_band_text) -> str:
    """Validate given query_band and append if required values missed in query_band."""
    # Ensures 'appname=dagster' and 'org=teradata-internal-telem' are in query_band_text.
    if query_band_text is not None:
        # checking org doesn't exist in query_band, appending 'org=teradata-internal-telem'
        #  If it exists, user might have set some value of their own, so doing nothing in that case
        pattern = r"org\s*=\s*([^;]*)"
        match = re.search(pattern, query_band_text)
        if not match:
            if not query_band_text.endswith(";"):
                query_band_text += ";"
            query_band_text += "org=teradata-internal-telem;"
        # Making sure appname in query_band contains 'dagster'
        pattern = r"appname\s*=\s*([^;]*)"
        # Search for the pattern in the query_band_text
        match = re.search(pattern, query_band_text)
        if match:
            appname_value = match.group(1).strip()
            # if appname exists and dagster not exists in appname then appending 'dagster' to existing
            # appname value
            if "dagster" not in appname_value.lower():
                new_appname_value = appname_value + "_dagster"
                # Optionally, you can replace the original value in the query_band_text
                updated_query_band_text = re.sub(
                    pattern, f"appname={new_appname_value}", query_band_text
                )
                query_band_text = updated_query_band_text
        else:
            # if appname doesn't exist in query_band, adding 'appname=dagster'
            if len(query_band_text.strip()) > 0 and not query_band_text.endswith(";"):
                query_band_text += ";"
            query_band_text += "appname=dagster;"
    else:
        query_band_text = "org=teradata-internal-telem;appname=dagster;"

    return query_band_text


class TeradataResource(ConfigurableResource, IAttachDifferentObjectToOpContext):
    host: Optional[str] = Field(description="Teradata Database Hostname")
    user: Optional[str] = Field(description="User login name.")
    password: Optional[str] = Field(description="User password.")
    database: Optional[str] = Field(
        default=None,
        description=("Name of the default database to use."),
    )
    query_band: Optional[str] = None
    port: Optional[str] = None
    tmode: Optional[str] = "ANSI"
    logmech: Optional[str] = None
    browser: Optional[str] = None
    browser_tab_timeout: Optional[int] = None
    browser_timeout: Optional[int] = None
    console_output_encoding: Optional[str] = Field(
        default="utf-8", description="Console output encoding."
    )
    bteq_session_encoding: Optional[str] = Field(
        default="ASCII", description="BTEQ session encoding."
    )
    bteq_output_width: Optional[int] = Field(
        default=65531, description="BTEQ output width."
    )
    bteq_quit_zero: Optional[bool] = Field(
        default=False, description="BTEQ quit zero flag."
    )
    http_proxy: Optional[str] = None
    http_proxy_user: Optional[str] = None
    http_proxy_password: Optional[str] = None
    https_proxy: Optional[str] = None
    https_proxy_user: Optional[str] = None
    https_proxy_password: Optional[str] = None
    proxy_bypass_hosts: Optional[str] = None
    sslmode: Optional[str] = None
    sslca: Optional[str] = None
    sslcapath: Optional[str] = None
    sslcrc: Optional[str] = None
    sslcipher: Optional[str] = None
    sslprotocol: Optional[str] = None
    slcrl: Optional[bool] = None
    sslocsp: Optional[bool] = None
    oidc_sslmode: Optional[str] = None

    @property
    @cached_method
    def _connection_args(self) -> Mapping[str, Any]:
        conn_args = {
            k: self._resolved_config_dict.get(k)
            for k in (
                "host",
                "user",
                "password",
                "database",
                "query_band",
                "port",
                "tmode",
                "logmech",
                "browser",
                "browser_tab_timeout",
                "browser_timeout",
                "http_proxy",
                "http_proxy_user",
                "http_proxy_password",
                "https_proxy",
                "https_proxy_user",
                "https_proxy_password",
                "proxy_bypass_hosts",
                "sslmode",
                "sslca",
                "sslcapath",
                "sslcrc",
                "sslcipher",
                "sslprotocol",
                "slcrl",
                "sslocsp",
                "oidc_sslmode",
            )
            if self._resolved_config_dict.get(k) is not None
        }
        return conn_args

    @public
    @contextmanager
    def get_connection(self):
        connection_params = {}
        if not self.host:
            raise ValueError("Host is required but not provided.")
        connection_params = {"host": self.host}

        if self.logmech is not None and self.logmech.lower() == "browser":
            # When logmech is "browser", username and password should not be provided.
            if self.user is not None or self.password is not None:
                raise ValueError(
                    "Username and password should not be specified when logmech is 'browser'"
                )
            if self.browser is not None:
                connection_params["browser"] = self.browser
            if self.browser_tab_timeout is not None:
                connection_params["browser_tab_timeout"] = str(self.browser_tab_timeout)
            if self.browser_timeout is not None:
                connection_params["browser_timeout"] = str(self.browser_timeout)
        else:
            if not self.user:
                raise ValueError("User is required but not provided.")
            if not self.password:
                raise ValueError("Password is required but not provided.")
            connection_params.update({"user": self.user, "password": self.password})
        if self.database is not None:
            connection_params["database"] = self.database
        if self.port is not None:
            connection_params["port"] = self.port
        if self.tmode is not None:
            connection_params["tmode"] = self.tmode
        if self.logmech is not None:
            connection_params["logmech"] = self.logmech
        if self.http_proxy is not None:
            connection_params["http_proxy"] = self.http_proxy
        if self.http_proxy_user is not None:
            connection_params["http_proxy_user"] = self.http_proxy_user
        if self.http_proxy_password is not None:
            connection_params["http_proxy_password"] = self.http_proxy_password
        if self.https_proxy is not None:
            connection_params["https_proxy"] = self.https_proxy
        if self.https_proxy_user is not None:
            connection_params["https_proxy_user"] = self.https_proxy_user
        if self.https_proxy_password is not None:
            connection_params["https_proxy_password"] = self.https_proxy_password
        if self.proxy_bypass_hosts is not None:
            connection_params["proxy_bypass_hosts"] = self.proxy_bypass_hosts
        if self.sslmode is not None:
            connection_params["sslmode"] = self.sslmode
        if self.sslca is not None:
            connection_params["sslca"] = self.sslca
        if self.sslcapath is not None:
            connection_params["sslcapath"] = self.sslcapath
        if self.sslcrc is not None:
            connection_params["sslcrc"] = self.sslcrc
        if self.sslcipher is not None:
            connection_params["sslcipher"] = self.sslcipher
        if self.sslprotocol is not None:
            connection_params["sslprotocol"] = self.sslprotocol
        if self.slcrl is not None:
            connection_params["slcrl"] = str(self.slcrl)
        if self.sslocsp is not None:
            connection_params["sslocsp"] = str(self.sslocsp)
        if self.oidc_sslmode is not None:
            connection_params["oidc_sslmode"] = self.oidc_sslmode

        teradata_conn = teradatasql.connect(**connection_params)
        self.set_query_band(self.query_band, teradata_conn)
        yield teradata_conn

    def set_query_band(self, query_band_text, teradata_conn):
        """Set SESSION Query Band for each connection session."""
        try:
            query_band_text = _handle_user_query_band_text(self, query_band_text)
            set_query_band_sql = f"SET QUERY_BAND='{query_band_text}' FOR SESSION"
            with teradata_conn.cursor() as cur:
                cur.execute(set_query_band_sql)
        except Exception:
            pass

    def get_object_to_set_on_execution_context(self) -> Any:
        # Directly create a TeradataDagsterConnection here for backcompat since the TeradataDagsterConnection
        # has methods this resource does not have
        return TeradataDagsterConnection(
            config=self._resolved_config_dict,
            log=get_dagster_logger(),
            teradata_connection_resource=self,
        )


class TeradataDagsterConnection:
    """A connection to Teradata that can execute queries. In general this class should not be
    directly instantiated, but rather used as a resource in an op or asset via the
    :py:func:`teradata_resource`.

    Note that the TeradataDagsterConnection is only used by the teradata_resource. The Pythonic TeradataResource does
    not use this TeradataDagsterConnection class.
    """

    def __init__(
        self,
        config: Mapping[str, str],
        log,
        teradata_connection_resource: TeradataResource,
    ):
        self.teradata_connection_resource = teradata_connection_resource
        self.compute_cluster_manager = TeradataComputeClusterManager(self, log)
        self.bteq = Bteq(self, teradata_connection_resource, log)
        self.log = log

    @public
    @contextmanager
    def get_connection(self):
        """Gets a connection to Teradata as a context manager.

        If using the execute_query or execute_queries methods,
        you do not need to create a connection using this context manager.

        Examples:
            .. code-block:: python

                @op(
                    required_resource_keys={"teradata"}
                )
                def execute_query(sql):
                    with context.resources.teradata.get_connection() as conn:
                        return conn.execute_query(sql, true, true)

        """
        with self.teradata_connection_resource.get_connection() as conn:
            yield conn

    @public
    def execute_query(
        self,
        sql: str,
        fetch_results: bool = False,
        single_result_row: bool = False,
    ):
        """Execute a query in Teradata.

        Args:
            sql (str): the query to be executed
            fetch_results (bool, optional): If True, fetch the query results. Defaults to False.
            single_result_row (bool, optional): If True, return only the first row of the result set.
                Effective only if `fetch_results` is True. Defaults to False.

        Examples:
            .. code-block:: python

                @op
                def get_database_info(teradata: TeradataResource):
                    teradata.execute_query("select * from dbc.dbcinfo")
        """
        check.str_param(sql, "sql")

        with self.get_connection() as conn:
            with closing(conn.cursor()) as cursor:
                # self.log.info("Executing query: " + sql)
                cursor.execute(sql)
                if fetch_results:
                    if single_result_row:
                        return self._single_result_row_handler(cursor)
                    else:
                        return cursor.fetchall()

    @public
    def execute_queries(
        self,
        sql_queries: Sequence[str],
        fetch_results: bool = False,
    ) -> Optional[Sequence[Any]]:
        """Execute multiple queries in Teradata.

        Args:
            sql_queries (Sequence[str]): List of queries to be executed in series
            fetch_results (bool, optional): If True, fetch the query results. Defaults to False.
            single_result_row (bool, optional): If True, return only the first row of the result set.
                Effective only if `fetch_results` is True. Defaults to False.

        Examples:
            .. code-block:: python

                @op
                def create_fresh_database(teradata: TeradataResource):
                    queries = ["DROP DATABASE MY_DATABASE", "CREATE DATABASE MY_DATABASE"]
                    teradata.execute_queries(
                        sql_queries=queries
                    )

        """
        check.sequence_param(sql_queries, "sql_queries", of_type=str)

        results: List[Any] = []
        with self.get_connection() as conn:
            with closing(conn.cursor()) as cursor:
                for sql in sql_queries:
                    # self.log.info("Executing query: " + sql)
                    cursor.execute(sql)
                    if fetch_results:
                        results = results.append(cursor.fetchall())  # type: ignore
                        return results

    def bteq_operator(
        self,
        sql: Optional[str] = None,
        file_path: Optional[str] = None,
        remote_host: Optional[str] = None,
        remote_user: Optional[str] = None,
        remote_password: Optional[str] = None,
        ssh_key_path: Optional[str] = None,
        remote_port: int = 22,
        remote_working_dir: str = "/tmp",
        bteq_script_encoding: Optional[str] = "utf-8",
        bteq_session_encoding: Optional[str] = "ASCII",
        bteq_quit_rc: Union[int, List[int]] = 0,
        timeout: int | Literal[600] = 600,  # Default to 10 minutes
        timeout_rc: int | None = None,
    ) -> Optional[int]:
        """
        Execute BTEQ commands either locally or on a remote machine via SSH.

        This method provides a unified interface to run BTEQ operations with various configurations:
        - Local or remote execution
        - Direct SQL input or file-based input
        - Custom encoding settings
        - Timeout controls
        - Authentication via password or SSH key

        Args:
            sql (Optional[str]): SQL commands to execute directly. Mutually exclusive with file_path.
            file_path (Optional[str]): [Optional] Path to an existing SQL or BTEQ script file.
            remote_host (Optional[str]): Hostname or IP for remote execution. None for local execution.
            remote_user (Optional[str]): Username for remote authentication. Required if remote_host specified.
            remote_password (Optional[str]): Password for remote authentication. Alternative to ssh_key_path.
            ssh_key_path (Optional[str]): Path to SSH private key for authentication. Alternative to remote_password.
            remote_port (int): SSH port for remote connection. Defaults to 22.
            remote_working_dir (str): Working directory on remote machine. Defaults to '/tmp'.
            bteq_script_encoding (Optional[str]): Encoding for BTEQ script file. Defaults to 'utf-8'.
            bteq_session_encoding (Optional[str]): Encoding for BTEQ session. Defaults to 'ASCII'.
            bteq_quit_rc (Union[int, List[int]]): Acceptable return codes for BTEQ execution. Defaults to 0.
            timeout (int | Literal[600]): Maximum execution time in seconds. Defaults to 600 (10 minutes).
            timeout_rc (int | None): Specific return code to use for timeout cases. Defaults to None.

        Returns:
            Optional[str]: The output of the BTEQ execution, or None if no output was produced.

        Raises:
            ValueError: If input validation fails, including:
                - Missing both sql and file_path
                - Providing both sql and file_path
                - Missing required remote authentication parameters
                - Invalid remote port specification
            DagsterError: If BTEQ execution fails or times out

        Note:
            - For remote execution, either remote_password or ssh_key_path must be provided (but not both)
            - Encoding handling follows these rules:
                * ASCII session encoding: Uses default system encoding
                * UTF8/UTF16 session encoding: Forces corresponding script encoding
            - Timeout handling:
                * MAXREQTIME parameter sets maximum execution time per request
                * EXITONDELAY forces exit if timeout occurs
                * Timeout return code can be customized

        Example:
            # Local execution with direct SQL
            >>> output = bteq_operator(sql="SELECT * FROM table;")

            # Remote execution with file
            >>> output = bteq_operator(
            ...     file_path="script.sql",
            ...     remote_host="example.com",
            ...     remote_user="user",
            ...     ssh_key_path="/path/to/key.pem"
            ... )
        """
        # Validate input parameters
        if not sql and not file_path:
            raise ValueError(
                "BteqOperator requires either the 'sql' or 'file_path' parameter. Both are missing."
            )

        if sum(bool(x) for x in [sql, file_path]) > 1:
            raise ValueError(
                "BteqOperator requires either the 'sql' or 'file_path' parameter but not both."
            )

        # Validate remote execution parameters if needed
        if remote_host:
            if not remote_user:
                raise ValueError("remote_user must be provided for remote execution")
            if not ssh_key_path and not remote_password:
                raise ValueError(
                    "Either ssh_key_path or remote_password must be provided for remote execution"
                )
            if remote_password and ssh_key_path:
                raise ValueError(
                    "Cannot specify both remote_password and ssh_key_path for remote execution"
                )

        # Validate network port
        if remote_port < 1 or remote_port > 65535:
            raise ValueError("remote_port must be a valid port number (1-65535)")

        # Handle encoding configuration
        temp_file_read_encoding = "UTF-8"
        if not bteq_session_encoding or bteq_session_encoding == "ASCII":
            bteq_session_encoding = ""
            if bteq_script_encoding == "UTF8":
                temp_file_read_encoding = "UTF-8"
            elif bteq_script_encoding == "UTF16":
                temp_file_read_encoding = "UTF-16"
            bteq_script_encoding = ""
        elif bteq_session_encoding == "UTF8" and (
            not bteq_script_encoding or bteq_script_encoding == "ASCII"
        ):
            bteq_script_encoding = "UTF8"
        elif bteq_session_encoding == "UTF16":
            if not bteq_script_encoding or bteq_script_encoding == "ASCII":
                bteq_script_encoding = "UTF8"

        # Map BTEQ encoding to Python file reading encoding
        if bteq_script_encoding == "UTF8":
            temp_file_read_encoding = "UTF-8"
        elif bteq_script_encoding == "UTF16":
            temp_file_read_encoding = "UTF-16"

        # Delegate execution to the underlying BTEQ implementation
        return self.bteq.bteq_operator(
            sql=sql,
            file_path=file_path,
            remote_host=remote_host,
            remote_user=remote_user,
            remote_password=remote_password,
            ssh_key_path=ssh_key_path,
            remote_port=remote_port,
            remote_working_dir=remote_working_dir,
            bteq_script_encoding=bteq_script_encoding,
            bteq_session_encoding=bteq_session_encoding,
            bteq_quit_rc=bteq_quit_rc,
            timeout=timeout,
            timeout_rc=timeout_rc,
            temp_file_read_encoding=temp_file_read_encoding,
        )

    def drop_database(self, databases: Union[str, Sequence[str]]) -> None:
        """
        Drop one or more databases in Teradata.

        Args:
            databases (Union[str, Sequence[str]]): Database name or list of database names to drop.

        Raises:
            Exception: Re-raises exceptions except for specific error codes.
        """
        if isinstance(databases, str):
            databases = [databases]

        with self.get_connection() as conn:
            with closing(conn.cursor()) as cursor:
                for database in databases:
                    try:
                        self.log.info(f"Dropping database: {database}")
                        cursor.execute(f"DROP DATABASE {database}")
                    except Exception as ex:
                        if "3802" in str(ex):  # Handle "database does not exist" error
                            self.log.warning(
                                f"Database '{database}' does not exist. Skipping."
                            )
                        else:
                            raise

    def drop_table(self, tables: Union[str, Sequence[str]]) -> None:
        """
        Drop one or more tables in Teradata.

        Args:
            tables (Union[str, Sequence[str]]): Table name or list of table names to drop.

        Raises:
            Exception: Re-raises exceptions except for specific error codes.
        """
        if isinstance(tables, str):
            tables = [tables]

        with self.get_connection() as conn:
            with closing(conn.cursor()) as cursor:
                for table in tables:
                    try:
                        self.log.info(f"Dropping table: {table}")
                        cursor.execute(f"DROP TABLE {table}")
                    except Exception as ex:
                        if "3807" in str(ex):  # Handle "table does not exist" error
                            self.log.warning(
                                f"Table '{table}' does not exist. Skipping."
                            )
                        else:
                            raise

    if TYPE_CHECKING:
        from dagster_aws.s3 import S3Resource

    @public
    def s3_to_teradata(
        self,
        s3: "S3Resource",
        s3_source_key: str,
        teradata_table: str,
        public_bucket: bool = False,
        teradata_authorization_name: str = "",
    ):
        """Loads CSV, JSON and Parquet format data from Amazon S3 to Teradata.

        Args:
            s3 (S3Resource): The S3Resource object used to interact with the S3 bucket.
            s3_source_key (str): The URI specifying the location of the S3 bucket. The URI format is
                /s3/YOUR-BUCKET.s3.amazonaws.com/YOUR-BUCKET-NAME.
                For more details, refer to:
                https://docs.teradata.com/search/documents?query=native+object+store&sort=last_update&virtual-field=title_only&content-lang=en-US.
            teradata_table (str): The name of the Teradata table to which the data will be loaded.
            public_bucket (bool): Indicates whether the provided S3 bucket is public. If true, the objects within
                the bucket can be accessed via a URL without authentication. If false, the bucket is considered private,
                and authentication must be provided. Default is False.
            teradata_authorization_name (str): The name of the Teradata Authorization Database Object, which controls access
                to the S3 object store. For more details, refer to:
                https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/Teradata-VantageTM-Native-Object-Store-Getting-Started-Guide-17.20/Setting-Up-Access/Controlling-Foreign-Table-Access-with-an-AUTHORIZATION-Object.

        Examples:
            .. code-block:: python

            import os
            import pytest
            from dagster import job, op
            from dagster_aws.s3 import S3Resource
            from dagster_teradata import TeradataResource, teradata_resource

            s3_resource = S3Resource(
                aws_access_key_id=os.getenv("aws_access_key_id"),
                aws_secret_access_key=os.getenv("aws_secret_access_key"),
                aws_session_token=os.getenv("aws_session_token")
            )

            teradata_resource = TeradataResource(
                host=os.getenv("TERADATA_HOST"),
                user=os.getenv("TERADATA_USER"),
                password=os.getenv("TERADATA_PASSWORD"),
                database=os.getenv("TERADATA_DATABASE")
            )


            def test_s3_to_teradata(tmp_path):
                @op(required_resource_keys={"teradata", "s3"})
                def example_test_s3_to_teradata(context):
                    context.resources.teradata.s3_to_teradata(
                        s3_resource, "/s3/mt255026-test.s3.amazonaws.com/people.csv", "people"
                    )

            @job(resource_defs={"teradata": teradata_resource, "s3": s3_resource})
            def example_job():
                example_test_s3_to_teradata()

            example_job.execute_in_process(resources={"s3": s3_resource, "teradata": teradata_resource})


        """
        credentials_part = "ACCESS_ID= '' ACCESS_KEY= ''"

        if not public_bucket:
            # Accessing data directly from the S3 bucket and creating permanent table inside the database
            if teradata_authorization_name:
                credentials_part = f"AUTHORIZATION={teradata_authorization_name}"
            else:
                access_key = s3.aws_access_key_id
                access_secret = s3.aws_secret_access_key
                token = s3.aws_session_token
                credentials_part = (
                    f"ACCESS_ID= '{access_key}' ACCESS_KEY= '{access_secret}'"
                )
                if token:
                    credentials_part = credentials_part + f" SESSION_TOKEN = '{token}'"

        sql = dedent(f"""
                    CREATE MULTISET TABLE {teradata_table} AS
                    (
                        SELECT * FROM (
                            LOCATION = '{s3_source_key}'
                            {credentials_part}
                        ) AS d
                    ) WITH DATA
                    """).rstrip()

        self.execute_query(sql)

    if TYPE_CHECKING:
        from dagster_azure.adls2 import ADLS2Resource

    @public
    def azure_blob_to_teradata(
        self,
        azure: "ADLS2Resource",
        blob_source_key: str,
        teradata_table: str,
        public_bucket: bool = False,
        teradata_authorization_name: str = "",
    ):
        """Loads CSV, JSON, and Parquet format data from Azure Blob Storage to Teradata.

        Args:
            azure (ADLS2Resource): The ADLS2Resource object used to interact with the Azure Blob .
            blob_source_key (str): The URI specifying the location of the Azure Blob object. The format is:
                `/az/YOUR-STORAGE-ACCOUNT.blob.core.windows.net/YOUR-CONTAINER/YOUR-BLOB-LOCATION`.
                Refer to the Teradata documentation for more details:
                https://docs.teradata.com/search/documents?query=native+object+store&sort=last_update&virtual-field=title_only&content-lang=en-US
            teradata_table (str): The name of the Teradata table where the data will be loaded.
            public_bucket (bool, optional): Indicates whether the Azure Blob container is public. If True,
                the objects in the container can be accessed without authentication. Defaults to False.
            teradata_authorization_name (str, optional): The name of the Teradata Authorization Database Object
                used to control access to the Azure Blob object store. This is required for secure access
                to private containers. Defaults to an empty string. Refer to the documentation:
                https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/Teradata-VantageTM-Native-Object-Store-Getting-Started-Guide-17.20/Setting-Up-Access/Controlling-Foreign-Table-Access-with-an-AUTHORIZATION-Object
        """
        credentials_part = "ACCESS_ID= '' ACCESS_KEY= ''"

        if not public_bucket:
            # Accessing data directly from the Azure Blob Storage and creating permanent table inside the database
            if teradata_authorization_name:
                credentials_part = f"AUTHORIZATION={teradata_authorization_name}"
            else:
                # Obtaining Azure client ID and secret from the ADLS2Resource resource
                azure_access_key = azure.storage_account
                azure_secret_key = azure.credential.token
                credentials_part = (
                    f"ACCESS_ID= '{azure_access_key}' ACCESS_KEY= '{azure_secret_key}'"
                )

        sql = dedent(f"""
                    CREATE MULTISET TABLE {teradata_table} AS
                    (
                        SELECT * FROM (
                            LOCATION = '{blob_source_key}'
                            {credentials_part}
                        ) AS d
                    ) WITH DATA
                    """).rstrip()

        self.execute_query(sql)

    # Handler to handle single result set of a SQL query
    def _single_result_row_handler(self, cursor):
        records = cursor.fetchone()
        if isinstance(records, list):
            return records[0]
        if records is None:
            return records
        raise TypeError(f"Unexpected results: {cursor.fetchone()!r}")

    def create_teradata_compute_cluster(
        self,
        compute_profile_name: str,
        compute_group_name: str,
        query_strategy: str = "STANDARD",
        compute_map: Optional[str] = None,
        compute_attribute: Optional[str] = None,
        timeout: int = constants.CC_OPR_TIME_OUT,
    ):
        return self.compute_cluster_manager.create_teradata_compute_cluster(
            compute_profile_name,
            compute_group_name,
            query_strategy,
            compute_map,
            compute_attribute,
            timeout,
        )

    def drop_teradata_compute_cluster(
        self,
        compute_profile_name: str,
        compute_group_name: str,
        delete_compute_group: bool = False,
    ):
        return self.compute_cluster_manager.drop_teradata_compute_cluster(
            compute_profile_name,
            compute_group_name,
            delete_compute_group,
        )

    def resume_teradata_compute_cluster(
        self,
        compute_profile_name: str,
        compute_group_name: str,
    ):
        return self.compute_cluster_manager.resume_teradata_compute_cluster(
            compute_profile_name,
            compute_group_name,
        )

    def suspend_teradata_compute_cluster(
        self,
        compute_profile_name: str,
        compute_group_name: str,
    ):
        return self.compute_cluster_manager.suspend_teradata_compute_cluster(
            compute_profile_name,
            compute_group_name,
        )


@dagster_maintained_resource
@resource(
    config_schema=TeradataResource.to_config_schema(),
    description="This resource is for connecting to the Teradata Vantage",
)
def teradata_resource(context) -> TeradataDagsterConnection:
    """A resource for connecting to the Teradata Vantage. The returned resource object is an
    instance of :py:class:`TeradataConnection`.

    A simple example of loading data into Teradata and subsequently querying that data is shown below:

    Examples:
        .. code-block:: python

            from dagster import job, op
            from dagster_teradata import teradata_resource

            @op(required_resource_keys={'teradata'})
            def get_one(context):
                context.resources.teradata.execute_query('SELECT 1')

            @job(resource_defs={'teradata': teradata_resource})
            def my_teradata_job():
                get_one()

            my_teradata_job.execute_in_process(
                run_config={
                    'resources': {
                        'teradata': {
                            'config': {
                                'host': {'env': 'TERADATA_HOST'},
                                'user': {'env': 'TERADATA_USER'},
                                'password': {'env': 'TERADATA_PASSWORD'},
                                'database': {'env': 'TERADATA_DATABASE'},
                            }
                        }
                    }
                }
            )
    """
    teradata_resource = TeradataResource.from_resource_context(context)
    return TeradataDagsterConnection(
        config=context, log=context.log, teradata_connection_resource=teradata_resource
    )


def fetch_last_updated_timestamps(
    *,
    teradata_connection: teradatasql.TeradataConnection,
    tables: Sequence[str],
    database: Optional[str] = None,
) -> Mapping[str, datetime]:
    """Fetch the last updated times of a list of tables in Teradata.

    If the underlying query to fetch the last updated time returns no results, a ValueError will be raised.

    Args:
        teradata_connection (teradatasql.TeradataConnection): A connection to Teradata.
            Accepts either a TeradataConnection or a sqlalchemy connection object,
            which are the two types of connections emittable from the teradata resource.
        schema (str): The schema of the tables to fetch the last updated time for.
        tables (Sequence[str]): A list of table names to fetch the last updated time for.
        database (Optional[str]): The database of the table. Only required if the connection
            has not been set with a database.

    Returns:
        Mapping[str, datetime]: A dictionary of table names to their last updated time in UTC.
    """
    check.invariant(
        len(tables) > 0, "Must provide at least one table name to query upon."
    )
    tables_str = ", ".join([f"'{table_name}'" for table_name in tables])
    fully_qualified_table_name = "DBC.TablesV"

    query = f"""
    SELECT TableName, CAST(LastAlterTimestamp AS TIMESTAMP(6)) AS LastAltered
    FROM {fully_qualified_table_name}
    WHERE DatabaseName = '{database}' AND TableName IN ({tables_str});
    """
    result = teradata_connection.cursor().execute(query)
    if not result:
        raise ValueError("No results returned from Teradata update time query.")

    result_mapping = {TableName: LastAltered for TableName, LastAltered in result}
    result_correct_case = {}
    for table_name in tables:
        if table_name not in result_mapping:
            raise ValueError(f"Table {table_name} could not be found.")
        last_altered = result_mapping[table_name]
        check.invariant(
            isinstance(last_altered, datetime),
            "Expected last_altered to be a datetime, but it was not.",
        )
        result_correct_case[table_name] = last_altered

    return result_correct_case

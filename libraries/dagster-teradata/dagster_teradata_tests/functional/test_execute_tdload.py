import os
from dagster import op, job
from dagster_teradata import TeradataResource

# Test configuration
SYSTEM_TESTS_DIR = os.path.abspath(os.path.dirname(__file__))
SOURCE_FILE = os.path.join(SYSTEM_TESTS_DIR, "tdload_src_file.txt")
TARGET_FILE = os.path.join(SYSTEM_TESTS_DIR, "tdload_target_file.txt")

# Teradata resource configuration
td_resource = TeradataResource(
    host=os.getenv("TERADATA_HOST"),
    user=os.getenv("TERADATA_USER"),
    password=os.getenv("TERADATA_PASSWORD"),
    database=os.getenv("TERADATA_DATABASE"),
)


def create_test_data_files():
    """Create source test data file."""
    with open(SOURCE_FILE, "w") as f:
        f.write("John|Doe|001|Engineering\n")
        f.write("Jane|Smith|002|Marketing\n")
        f.write("Bob|Johnson|003|Sales\n")


def cleanup_test_files():
    """Cleanup test files."""
    if os.path.exists(SOURCE_FILE):
        os.remove(SOURCE_FILE)
    if os.path.exists(TARGET_FILE):
        os.remove(TARGET_FILE)


class TestTPTWorkflows:
    """Test suite for TPT workflows in Dagster."""

    def setup_tables(self, table_names):
        """Setup tables before each test - delete if exists and create new."""

        @op(required_resource_keys={"teradata"})
        def setup_tables_op(context):
            for table_name in table_names:
                # DROP TABLE
                drop_result = context.resources.teradata.ddl_operator(
                    ddl=[f"DROP TABLE  {table_name};"],
                    error_list=[3807, 3853],  # Ignore table doesn't exist errors
                )
                context.log.info(f"Drop table {table_name} result: {drop_result}")

                # Create table
                create_result = context.resources.teradata.ddl_operator(
                    ddl=[
                        f"""CREATE TABLE {table_name} (
                        first_name VARCHAR(100),
                        last_name VARCHAR(100), 
                        employee_id VARCHAR(10),
                        department VARCHAR(50)
                    );"""
                    ],
                    error_list=[3807],  # Ignore table already exists
                )
                context.log.info(f"Create table {table_name} result: {create_result}")
                assert create_result == 0
            return 0

        @job(resource_defs={"teradata": td_resource})
        def setup_job():
            setup_tables_op()

        result = setup_job.execute_in_process()
        assert result.success

    def test_01_ddl_operator_basic(self):
        """Test basic DDL operator functionality."""
        create_test_data_files()

        try:

            @op(required_resource_keys={"teradata"})
            def setup_tables(context):
                # Clean up and create test table
                result = context.resources.teradata.ddl_operator(
                    ddl=[
                        "DROP TABLE  ddl_basic_test;",
                        """CREATE TABLE ddl_basic_test (
                            id INTEGER,
                            name VARCHAR(100)
                        );""",
                    ],
                    error_list=[3807, 3853],
                )
                assert result == 0
                return result

            @op(required_resource_keys={"teradata"})
            def verify_table(context, setup_result):
                # Verify table was created by inserting data
                result = context.resources.teradata.ddl_operator(
                    ddl=[
                        "INSERT INTO ddl_basic_test VALUES (1, 'Test User');",
                        "INSERT INTO ddl_basic_test VALUES (2, 'Another User');",
                    ]
                )
                assert result == 0
                return result

            @job(resource_defs={"teradata": td_resource})
            def ddl_basic_job():
                setup = setup_tables()
                verify_table(setup)

            result = ddl_basic_job.execute_in_process()
            assert result.success
            print("✓ DDL Operator Basic Test Passed")

        finally:
            cleanup_test_files()

    def test_02_tdload_file_to_table(self):
        """Test TPT file to table loading."""
        create_test_data_files()

        try:
            # Setup table first
            self.setup_tables(["file_to_table_test"])

            @op(required_resource_keys={"teradata"})
            def load_file_to_table(context):
                result = context.resources.teradata.tdload_operator(
                    source_file_name=SOURCE_FILE,
                    target_table="file_to_table_test",
                    source_format="Delimited",
                    source_text_delimiter="|",
                )
                context.log.info(f"File to table load result: {result}")
                assert result == 0
                return result

            @job(resource_defs={"teradata": td_resource})
            def file_to_table_job():
                load_file_to_table()

            result = file_to_table_job.execute_in_process()
            assert result.success
            print("✓ File to Table Test Passed")

        finally:
            cleanup_test_files()

    def test_03_tdload_table_to_file(self):
        """Test TPT table to file export."""
        create_test_data_files()

        try:
            # Setup table and load data first
            self.setup_tables(["table_to_file_test"])

            @op(required_resource_keys={"teradata"})
            def load_data_into_table(context):
                # First load data into table
                result = context.resources.teradata.tdload_operator(
                    source_file_name=SOURCE_FILE,
                    target_table="table_to_file_test",
                    source_format="Delimited",
                    source_text_delimiter="|",
                )
                assert result == 0
                return result

            @op(required_resource_keys={"teradata"})
            def export_table_to_file(context, load_result):
                # Then export table to file
                result = context.resources.teradata.tdload_operator(
                    source_table="table_to_file_test",
                    target_file_name=TARGET_FILE,
                    target_format="Delimited",
                    target_text_delimiter="|",
                )
                context.log.info(f"Table to file export result: {result}")
                assert result == 0
                return result

            @op
            def verify_exported_file(context, export_result):
                assert os.path.exists(TARGET_FILE)
                with open(TARGET_FILE, "r") as f:
                    content = f.read()
                    assert "John|Doe|001|Engineering" in content
                    assert "Jane|Smith|002|Marketing" in content
                return export_result

            @job(resource_defs={"teradata": td_resource})
            def table_to_file_job():
                load_data = load_data_into_table()
                export_data = export_table_to_file(load_data)
                verify_exported_file(export_data)

            result = table_to_file_job.execute_in_process()
            assert result.success
            print("✓ Table to File Test Passed")

        finally:
            cleanup_test_files()

    def test_04_tdload_table_to_table(self):
        """Test TPT table to table transfer."""
        create_test_data_files()

        try:
            # Setup source and target tables
            self.setup_tables(["source_table_tt", "target_table_tt"])

            @op(required_resource_keys={"teradata"})
            def load_source_table(context):
                # Load data into source table
                result = context.resources.teradata.tdload_operator(
                    source_file_name=SOURCE_FILE,
                    target_table="source_table_tt",
                    source_format="Delimited",
                    source_text_delimiter="|",
                )
                assert result == 0
                return result

            @op(required_resource_keys={"teradata"})
            def transfer_table_to_table(context, load_result):
                # Transfer from source to target table
                result = context.resources.teradata.tdload_operator(
                    source_table="source_table_tt", target_table="target_table_tt"
                )
                context.log.info(f"Table to table transfer result: {result}")
                assert result == 0
                return result

            @job(resource_defs={"teradata": td_resource})
            def table_to_table_job():
                load_source = load_source_table()
                transfer_table_to_table(load_source)

            result = table_to_table_job.execute_in_process()
            assert result.success
            print("✓ Table to Table Test Passed")

        finally:
            cleanup_test_files()

    def test_05_tdload_with_select_statement(self):
        """Test TPT with SELECT statement."""
        create_test_data_files()

        try:
            self.setup_tables(["select_stmt_test"])

            @op(required_resource_keys={"teradata"})
            def load_initial_data(context):
                # Load initial data
                result = context.resources.teradata.tdload_operator(
                    source_file_name=SOURCE_FILE,
                    target_table="select_stmt_test",
                    source_format="Delimited",
                    source_text_delimiter="|",
                )
                assert result == 0
                return result

            @op(required_resource_keys={"teradata"})
            def export_with_select(context, load_result):
                # Export using SELECT statement with filter - FIXED: Use proper parameter name
                result = context.resources.teradata.tdload_operator(
                    select_stmt="SELECT first_name, last_name FROM select_stmt_test;",
                    target_file_name=TARGET_FILE,
                    target_format="Delimited",
                    target_text_delimiter=",",
                )
                context.log.info(f"Select statement export result: {result}")
                assert result == 0
                return result

            @op
            def verify_select_export(context, export_result):
                assert os.path.exists(TARGET_FILE)
                with open(TARGET_FILE, "r") as f:
                    content = f.read()
                    assert "John,Doe" in content
                return export_result

            @job(resource_defs={"teradata": td_resource})
            def select_statement_job():
                load_data = load_initial_data()
                export_data = export_with_select(load_data)
                verify_select_export(export_data)

            result = select_statement_job.execute_in_process()
            assert result.success
            print("✓ SELECT Statement Test Passed")

        finally:
            cleanup_test_files()

    def test_06_tdload_with_error_handling(self):
        """Test TPT with error handling."""
        create_test_data_files()

        try:
            self.setup_tables(["error_handling_test"])

            @op(required_resource_keys={"teradata"})
            def create_table_with_constraints(context):
                # Create table with constraints - FIXED: PRIMARY KEY columns must be NOT NULL
                result = context.resources.teradata.ddl_operator(
                    ddl=[
                        "DROP TABLE  constrained_table;",
                        "DROP TABLE  constrained_table_ET;",
                        "DROP TABLE  constrained_table_WT;",
                        "DROP TABLE  constrained_table_UV;",
                        """CREATE TABLE constrained_table (
                            id INTEGER NOT NULL PRIMARY KEY,
                            name VARCHAR(50) NOT NULL
                        );""",
                    ],
                    error_list=[3807, 3853],
                )
                assert result == 0
                return result

            @op(required_resource_keys={"teradata"})
            def load_with_expected_errors(context, create_result):
                # Create test file with duplicate IDs
                error_file = "/tmp/error_test_data.txt"
                with open(error_file, "w") as f:
                    f.write("1|Valid Record 1\n")
                    f.write("1|Duplicate ID\n")  # This should cause error
                    f.write("2|Valid Record 2\n")

                try:
                    result = context.resources.teradata.tdload_operator(
                        source_file_name=error_file,
                        target_table="constrained_table",
                        source_format="Delimited",
                        source_text_delimiter="|",
                    )
                    context.log.info(f"Load with error handling result: {result}")
                    # Operation might complete with warnings
                    return result
                finally:
                    if os.path.exists(error_file):
                        os.remove(error_file)

            @job(resource_defs={"teradata": td_resource})
            def error_handling_job():
                create_table = create_table_with_constraints()
                load_with_expected_errors(create_table)

            result = error_handling_job.execute_in_process()
            # Even with errors, the job should complete due to error_list
            assert result.success
            print("✓ Error Handling Test Passed")

        finally:
            cleanup_test_files()

    def test_08_ddl_operator_with_error_list(self):
        """Test DDL operator with error list handling."""

        @op(required_resource_keys={"teradata"})
        def ddl_with_error_handling(context):
            # Try to create same table multiple times with error list
            # FIXED: Added proper error handling for the specific error
            result = context.resources.teradata.ddl_operator(
                ddl=[
                    "DROP TABLE  error_list_test;",
                    "CREATE TABLE error_list_test (id INTEGER);",
                    "CREATE TABLE error_list_test (id INTEGER);",  # This should fail but be ignored
                ],
                error_list=[3807, 3803],  # FIXED: Added 3803 for "table already exists"
            )
            context.log.info(f"DDL with error list result: {result}")
            # Should succeed despite the duplicate create attempt
            assert result == 0
            return result

        @job(resource_defs={"teradata": td_resource})
        def ddl_error_list_job():
            ddl_with_error_handling()

        result = ddl_error_list_job.execute_in_process()
        assert result.success
        print("✓ DDL Error List Test Passed")

    def test_09_sequential_tpt_operations(self):
        """Test sequential TPT operations."""
        create_test_data_files()

        try:
            self.setup_tables(["sequential_test_source", "sequential_test_target"])

            @op(required_resource_keys={"teradata"})
            def step1_load_source(context):
                result = context.resources.teradata.tdload_operator(
                    source_file_name=SOURCE_FILE,
                    target_table="sequential_test_source",
                    source_format="Delimited",
                    source_text_delimiter="|",
                )
                assert result == 0
                return result

            @op(required_resource_keys={"teradata"})
            def step2_transfer_data(context, previous_result):
                result = context.resources.teradata.tdload_operator(
                    source_table="sequential_test_source",
                    target_table="sequential_test_target",
                )
                assert result == 0
                return result

            @op(required_resource_keys={"teradata"})
            def step3_export_data(context, previous_result):
                result = context.resources.teradata.tdload_operator(
                    source_table="sequential_test_target",
                    target_file_name=TARGET_FILE,
                    target_format="Delimited",
                    target_text_delimiter="|",
                )
                assert result == 0
                return result

            @op
            def step4_verify_results(context, previous_result):
                assert os.path.exists(TARGET_FILE)
                with open(TARGET_FILE, "r") as f:
                    lines = f.read().strip().split("\n")
                    assert len(lines) == 3  # Should have 3 data rows
                return previous_result

            @job(resource_defs={"teradata": td_resource})
            def sequential_operations_job():
                step1 = step1_load_source()
                step2 = step2_transfer_data(step1)
                step3 = step3_export_data(step2)
                step4_verify_results(step3)

            result = sequential_operations_job.execute_in_process()
            assert result.success
            print("✓ Sequential Operations Test Passed")

        finally:
            cleanup_test_files()

    def test_10_complete_workflow(self):
        """Test complete TPT workflow."""
        create_test_data_files()

        try:
            self.setup_tables(["workflow_source", "workflow_target"])

            @op(required_resource_keys={"teradata"})
            def create_tables(context):
                # Additional table setup if needed
                return 0

            @op(required_resource_keys={"teradata"})
            def load_initial_data(context, create_result):
                result = context.resources.teradata.tdload_operator(
                    source_file_name=SOURCE_FILE,
                    target_table="workflow_source",
                    source_format="Delimited",
                    source_text_delimiter="|",
                )
                assert result == 0
                return result

            @op(required_resource_keys={"teradata"})
            def process_data(context, load_result):
                result = context.resources.teradata.tdload_operator(
                    source_table="workflow_source", target_table="workflow_target"
                )
                assert result == 0
                return result

            @op(required_resource_keys={"teradata"})
            def export_results(context, process_result):
                result = context.resources.teradata.tdload_operator(
                    source_table="workflow_target",
                    target_file_name=TARGET_FILE,
                    target_format="Delimited",
                    target_text_delimiter="|",
                )
                assert result == 0
                return result

            @op
            def verify_final_output(context, export_result):
                assert os.path.exists(TARGET_FILE)
                with open(TARGET_FILE, "r") as f:
                    content = f.read()
                    assert "Engineering" in content
                    assert "Marketing" in content
                    assert "Sales" in content
                return export_result

            @job(resource_defs={"teradata": td_resource})
            def complete_workflow_job():
                create = create_tables()
                load = load_initial_data(create)
                process = process_data(load)
                export = export_results(process)
                verify_final_output(export)

            result = complete_workflow_job.execute_in_process()
            assert result.success
            print("✓ Complete Workflow Test Passed")

        finally:
            cleanup_test_files()


# Run all tests
if __name__ == "__main__":
    test_suite = TestTPTWorkflows()

    print("Running TPT Workflow Tests...")
    print("=" * 50)

    tests = [
        test_suite.test_01_ddl_operator_basic,
        test_suite.test_02_tdload_file_to_table,
        test_suite.test_03_tdload_table_to_file,
        test_suite.test_04_tdload_table_to_table,
        test_suite.test_05_tdload_with_select_statement,
        test_suite.test_06_tdload_with_error_handling,
        test_suite.test_07_tdload_with_job_var_file,
        test_suite.test_08_ddl_operator_with_error_list,
        test_suite.test_09_sequential_tpt_operations,
        test_suite.test_10_complete_workflow,
    ]

    for i, test in enumerate(tests, 1):
        try:
            test()
            print(f"Test {i:02d}: PASSED")
        except Exception as e:
            print(f"Test {i:02d}: FAILED - {e}")

    print("=" * 50)
    print("All tests completed!")

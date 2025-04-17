import datetime as dt
import random
import shutil
import subprocess
from collections.abc import Iterator

import psycopg2
import pyarrow as pa
import pytest
from dagster._utils import file_relative_path
from pyiceberg.catalog import Catalog, load_catalog

COMPOSE_DIR = file_relative_path(__file__, "docker")

POSTGRES_USER = "test"
POSTGRES_PASSWORD = "test"
POSTGRES_DB = "test"
POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432

WAREHOUSE_DIR = "warehouse"


@pytest.fixture(scope="session", autouse=True)
def compose(tmp_path_factory: pytest.TempPathFactory) -> Iterator[None]:
    # Determine the warehouse path temporary directory pytest will make:
    # https://github.com/pytest-dev/pytest/blob/b48e23d/src/_pytest/tmpdir.py#L67
    warehouse_path = str(
        tmp_path_factory.getbasetemp().joinpath(WAREHOUSE_DIR).resolve()
    )

    subprocess.run(
        ["docker", "compose", "up", "--build", "--wait"],
        cwd=COMPOSE_DIR,
        check=True,
        env={"WAREHOUSE_PATH": warehouse_path},
    )
    subprocess.run(["sleep", "10"])
    yield
    subprocess.run(
        ["docker", "compose", "down", "--remove-orphans", "--volumes"],
        cwd=COMPOSE_DIR,
        check=True,
        env={"WAREHOUSE_PATH": warehouse_path},
    )


@pytest.fixture(scope="session")
def postgres_connection() -> Iterator[psycopg2.extensions.connection]:
    conn = psycopg2.connect(
        database=POSTGRES_DB,
        port=POSTGRES_PORT,
        host=POSTGRES_HOST,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def postgres_uri() -> str:
    return f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"


# NB: we truncate all iceberg tables before each test
#  that way, we don't have to worry about side effects
@pytest.fixture(autouse=True)
def clean_iceberg_tables(postgres_connection: psycopg2.extensions.connection):
    with postgres_connection.cursor() as cur:
        cur.execute(
            "SELECT tablename FROM pg_catalog.pg_tables WHERE tablename LIKE 'iceberg%';",
        )
        for tbl in cur.fetchall():
            cur.execute(f"TRUNCATE TABLE {tbl[0]};")
    postgres_connection.commit()


# NB: recreated for every test
@pytest.fixture(autouse=True)
def warehouse_path(tmp_path_factory: pytest.TempPathFactory) -> str:
    # Determine the warehouse path temporary directory pytest will make:
    # https://github.com/pytest-dev/pytest/blob/b48e23d/src/_pytest/tmpdir.py#L67
    # TODO(deepyaman): Figure out why teardown isn't called in cloud CI.
    if (old_dir := tmp_path_factory.getbasetemp().joinpath(WAREHOUSE_DIR)).exists():
        shutil.rmtree(old_dir)

    dir_ = tmp_path_factory.mktemp(WAREHOUSE_DIR, numbered=False)
    yield str(dir_.resolve())
    shutil.rmtree(dir_)


@pytest.fixture
def catalog_config_properties(warehouse_path: str, postgres_uri: str) -> dict[str, str]:
    return {
        "uri": postgres_uri,
        "warehouse": f"file://{warehouse_path!s}",
    }


@pytest.fixture(scope="session")
def catalog_name() -> str:
    return "postgres"


@pytest.fixture
def catalog(catalog_name: str, catalog_config_properties: dict[str, str]) -> Catalog:
    return load_catalog(
        name=catalog_name,
        **catalog_config_properties,
    )


@pytest.fixture(scope="session")
def namespace_name() -> str:
    return "pytest"


@pytest.fixture(autouse=True)
def namespace(catalog: Catalog, namespace_name: str) -> str:
    catalog.create_namespace(namespace_name)
    return namespace_name


@pytest.fixture(scope="session")
def data() -> pa.Table:
    random.seed(876)
    N = 1440
    d = {
        "timestamp": pa.array(
            [
                dt.datetime(2023, 1, 1, 0, 0, 0) + dt.timedelta(minutes=i)
                for i in range(N)
            ],
        ),
        "category": pa.array([random.choice(["A", "B", "C"]) for _ in range(N)]),
        "value": pa.array(random.uniform(0, 1) for _ in range(N)),
    }
    return pa.Table.from_pydict(d)


@pytest.fixture(scope="session")
def data_schema(data: pa.Table) -> pa.Schema:
    return data.schema

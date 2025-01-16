import subprocess
import time
from pathlib import Path

from dagster import asset, materialize
from dagster_chroma import ChromaResource, LocalConfig, HttpConfig


def test_local_resource(tmp_path: Path):
    @asset
    def create_collection(chroma_local: ChromaResource):
        with chroma_local.get_client() as chroma_client:
            collection = chroma_client.create_collection("fruits")

            collection.add(
                documents=[
                    "This is a document about oranges",
                    "This is a document about pineapples",
                    "This is a document about strawberries",
                    "This is a document about cucumbers",
                ],
                ids=["oranges", "pineapples", "strawberries", "cucumbers"],
            )

    @asset
    def query_collection(chroma_local: ChromaResource):
        with chroma_local.get_client() as chroma_client:
            collection = chroma_client.get_collection("fruits")

            results = collection.query(query_texts=["hawaii"], n_results=1)

            assert results["ids"][0][0] == "pineapples"

    result = materialize(
        [create_collection, query_collection],
        resources={
            "chroma_local": ChromaResource(
                connection_config=LocalConfig(
                    persistence_path=str(tmp_path),
                )
            )
        },
    )

    assert result.success


HTTP_PORT = 8001


def test_http_resource(tmp_path: Path):
    @asset
    def create_collection(chroma_http: ChromaResource):
        with chroma_http.get_client() as chroma_client:
            collection = chroma_client.create_collection("fruits")

            collection.add(
                documents=[
                    "This is a document about oranges",
                    "This is a document about pineapples",
                    "This is a document about strawberries",
                    "This is a document about cucumbers",
                ],
                ids=["oranges", "pineapples", "strawberries", "cucumbers"],
            )

    @asset
    def query_collection(chroma_http: ChromaResource):
        with chroma_http.get_client() as chroma_client:
            collection = chroma_client.get_collection("fruits")

            results = collection.query(query_texts=["hawaii"], n_results=1)

            assert results["ids"][0][0] == "pineapples"

    chroma_server = subprocess.Popen(
        [
            "chroma",
            "run",
            "--path",
            str(tmp_path.resolve().absolute()),
            "--port",
            str(HTTP_PORT),
        ],
        cwd=tmp_path,
    )

    try:
        time.sleep(4)  # Give the server time to start

        result = materialize(
            [create_collection, query_collection],
            resources={
                "chroma_http": ChromaResource(
                    connection_config=HttpConfig(host="localhost", port=HTTP_PORT)
                )
            },
        )

        assert result.success

    finally:
        chroma_server.kill()  # Make sure not to leave the server running after the test

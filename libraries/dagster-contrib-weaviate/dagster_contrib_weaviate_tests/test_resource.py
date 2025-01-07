import time
import json
from typing import List, Any
from pathlib import Path
import os

import weaviate

from dagster import asset, materialize
from dagster_contrib_weaviate import WeaviateLocalResource

WEAVIATE_PORT = 8079
WEAVIATE_GRPC_PORT = 50050

DATA_DIR = Path(os.path.dirname(__file__))


def read_data_vector_from_file() -> List[Any]:
    with open(Path(DATA_DIR, "data_vector.json")) as f:
        return json.load(f)


def read_query_vector_from_file() -> List[float]:
    with open(Path(DATA_DIR, "query_vector.json")) as f:
        return json.load(f)


def create_embedded_weaviate_and_add_data() -> weaviate.WeaviateClient:
    """Creates an embedded instance of weaviate, and adds data (including vectors), from
    the Weaviate tutorial. https://weaviate.io/developers/weaviate/starter-guides/custom-vectors
    The returned WeaviateClient should be closed when the Weaviate instance is no longer needed
    """

    data_vector = read_data_vector_from_file()

    client = weaviate.connect_to_embedded(
        hostname="localhost", port=WEAVIATE_PORT, grpc_port=WEAVIATE_GRPC_PORT
    )

    question_objs = list()
    for i, d in enumerate(data_vector):
        question_objs.append(
            weaviate.classes.data.DataObject(
                properties={
                    "answer": d["Answer"],
                    "question": d["Question"],
                    "category": d["Category"],
                },
                vector=d["vector"],
            )
        )

    questions = client.collections.get("Question")
    questions.data.insert_many(question_objs)

    time.sleep(
        1
    )  # Weaviate recommends a short sleep here, due to async operations running in the background.

    return client


def test_local_resource():
    """Starts up an embedded instance of Weaviate, adds some data, and then queries it.
    To avoid installing heavy models on github-actions (which could incur costs),
    we use the "bring-your-own-vectors" method,
    as described here: https://weaviate.io/developers/weaviate/starter-guides/custom-vectors
    """

    @asset
    def query_weaviate_asset(weaviate_resource: WeaviateLocalResource):
        with create_embedded_weaviate_and_add_data():
            with weaviate_resource.get_client() as client:
                questions = client.collections.get("Question")
                response = questions.query.near_vector(
                    near_vector=read_query_vector_from_file(),
                    limit=2,
                    return_metadata=weaviate.classes.query.MetadataQuery(
                        certainty=True
                    ),
                )

                assert len(response.objects) == 2

    result = materialize(
        [query_weaviate_asset],
        resources={
            "weaviate_resource": WeaviateLocalResource(
                host="localhost", port=WEAVIATE_PORT, grpc_port=WEAVIATE_GRPC_PORT
            )
        },
    )

    assert result.success

import json
from typing import Any
from pathlib import Path
from unittest import mock
import os
import httpx
import time

import weaviate

from dagster import asset, materialize
from dagster_weaviate import WeaviateResource, LocalConfig, CloudConfig

WEAVIATE_PORT = 8079
WEAVIATE_GRPC_PORT = 50050

DATA_DIR = Path(os.path.dirname(__file__))


def read_data_vector_from_file() -> list[Any]:
    with open(Path(DATA_DIR, "data_vector.json")) as f:
        return json.load(f)


def read_query_vector_from_file() -> list[float]:
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
    def query_local_weaviate_asset(weaviate_local: WeaviateResource):
        with create_embedded_weaviate_and_add_data():
            with weaviate_local.get_client() as client:
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
        [query_local_weaviate_asset],
        resources={
            "weaviate_local": WeaviateResource(
                connection_config=LocalConfig(
                    host="localhost", port=WEAVIATE_PORT, grpc_port=WEAVIATE_GRPC_PORT
                )
            )
        },
    )

    assert result.success


WCD_URL = "https://rand0md1g1t5.something.weaviate.cloud"
WCD_APIKEY = "FakeWeaviateCloudApiKey"
COHERE_APIKEY = "FakeCohereApiKey"

# Required to avoid an infinite recursion
# (since our mock needs to forward some requests to the original httpx.Send)
real_httpx_send_method = httpx.AsyncClient.send


# this method only returns a mock response if the URL accessed is weaviate.
# Otherwise, it passes the request through to real_httpx_send_method
# Important since Weaviate might make some PyPI requests (which don't need to be mocked).
async def mock_httpx_send_method(self, *args, **kwargs):
    request = args[0]

    if "weaviate.cloud" not in str(request.url):
        return await real_httpx_send_method(self, *args, **kwargs)

    assert WCD_URL in str(request.url)
    assert ("x-cohere-api-key", COHERE_APIKEY) in request.headers.items()
    assert WCD_APIKEY in request.headers["authorization"]

    # The mock response is based on:
    # export WCD_API_KEY=...
    # export WCD_URL=...
    # curl $WCD_URL/v1/meta \
    #   -H "x-weaviate-api-key: $WCD_API_KEY" \
    #   -H "x-weaviate-cluster-url: $WCD_URL" \
    #   -H "authorization: Bearer $WCD_API_KEY"
    result = httpx.Response(
        status_code=200,
        text=json.dumps(
            {
                "grpcMaxMessageSize": 10485760,
                "hostname": "http://[::]:8080",
                "modules": {
                    "backup-gcs": {
                        "bucketName": "weaviate-wcs-example-example-backup",
                        "rootName": "395b9713-2bb5-4a12-a9bc-a32eb6a2a3d8",  # fake UUID
                    },
                    "generative-anthropic": {
                        "documentationHref": "https://docs.anthropic.com/en/api/getting-started",
                        "name": "Generative Search - Anthropic",
                    },
                    "text2vec-voyageai": {
                        "documentationHref": "https://docs.voyageai.com/docs/embeddings",
                        "name": "VoyageAI Module",
                    },
                    "text2vec-weaviate": {
                        "documentationHref": "https://api.embedding.weaviate.io",
                        "name": "Weaviate Embedding Module",
                    },
                },
                "version": "1.28.2",
            }
        ),
    )

    return result


def test_cloud_resource():
    """Mocks the httpx.AsyncClient.send method.
    Makes sure the url and headers (incl' authorization) of the request made by Weaviate are the same as
    specified in the Resource config.
    """
    with mock.patch.object(
        httpx.AsyncClient, "send", autospec=True, side_effect=mock_httpx_send_method
    ):

        @asset
        def query_cloud_weaviate_asset(weaviate_cloud: WeaviateResource):
            with weaviate_cloud.get_client():
                # We only test the basic connection flow - which is over http (httpx library).
                # The bulk of Weaviate communication (ie queries) is over grpc, which is quite complex to mock.
                pass

        result = materialize(
            [query_cloud_weaviate_asset],
            resources={
                "weaviate_cloud": WeaviateResource(
                    connection_config=CloudConfig(cluster_url=WCD_URL),
                    auth_credentials={"api_key": WCD_APIKEY},
                    headers={
                        "X-Cohere-Api-Key": COHERE_APIKEY,
                    },
                    # Important so that Weaviate doesn't make gRPC "heartbeat" checks
                    # on inititalization. (Those will fail here,, of course, since in the mock-tests we
                    # don't use a real api-key).
                    skip_init_checks=True,
                ),
            },
        )

        assert result.success

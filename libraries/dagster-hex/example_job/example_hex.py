# resources.py
import os

from dagster import job

from dagster_hex.ops import hex_project_op
from dagster_hex.resources import hex_resource

API_KEY = os.getenv("DAGSTER_HEX_API")
PROJ_ID = "db753fd1-2b7c-4dbe-8cbb-30742214cd9b"

notifications = [
    {
        "type": "SUCCESS",
        "includeSuccessScreenshot": True,
        "slackChannelIds": ["C059NUK6SQG"],
        "userIds": [],
        "groupIds": [],
    }
]


my_resource = hex_resource.configured({"api_key": API_KEY})
run_hex_op = hex_project_op.configured(
    {"project_id": PROJ_ID, "notifications": notifications}, name="run_job"
)


@job(resource_defs={"hex": my_resource})
def hex_job():
    run_hex_op()

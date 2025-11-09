from typing import NamedTuple, TypedDict


class RunResponse(TypedDict):
    projectId: str
    runId: str
    runUrl: str
    runStatusUrl: str
    traceId: str


class ProjectResponse(TypedDict):
    projectId: str
    title: str
    description: str
    creator: dict
    lastEditedAt: str
    lastPublishedAt: str
    categories: list[dict]


class NotificationResponse(TypedDict):
    type: str
    recipientType: str
    includeSuccessScreenshot: bool
    recipients: list[dict]


class StatusResponse(TypedDict):
    projectId: str
    runId: str
    runUrl: str
    status: str
    startTime: str
    endTime: str
    elapsedTime: int
    traceId: str
    notifications: list[NotificationResponse]


class NotificationDetails(TypedDict):
    type: str
    includeSuccessScreenshot: bool
    slackChannelIds: list[str]
    userIds: list[str]
    groupIds: list[str]


class HexOutput(
    NamedTuple(
        "_HexOutput",
        [
            ("run_response", RunResponse),
            ("status_response", StatusResponse),
        ],
    )
):
    pass

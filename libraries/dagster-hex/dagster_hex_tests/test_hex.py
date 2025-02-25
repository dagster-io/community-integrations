from dagster_hex.resources import HexResource


def test_resource_request(requests_mock):
    requests_mock.get(
        "https://testurl/someendpoint",
        headers={"Content-Type": "application/json"},
        json={"data": "success"},
    )
    hex = HexResource(api_key="abc", base_url="https://testurl/")
    res = hex.make_request(method="GET", endpoint="/someendpoint")
    assert res == {"data": "success"}


def test_run_project(requests_mock):
    requests_mock.post(
        "https://testurl/api/v1/project/abc-123/run",
        headers={"Content-Type": "application/json"},
        json={"data": "mocked response"},
    )

    hex = HexResource(api_key="abc", base_url="https://testurl/")
    response = hex.run_project("abc-123", inputs={"param": "var"})
    assert response == {"data": "mocked response"}
    assert requests_mock.last_request.json() == {
        "inputParams": {"param": "var"},
        "useCachedSqlResults": True,
        "dryRun": False,
        "updatePublishedResults": False,
    }


def test_run_project_no_input(requests_mock):
    requests_mock.post(
        "https://testurl/api/v1/project/abc-123/run",
        headers={"Content-Type": "application/json"},
        json={"data": "mocked response"},
    )

    hex = HexResource(api_key="abc", base_url="https://testurl/")
    response = hex.run_project("abc-123")
    assert response == {"data": "mocked response"}
    assert requests_mock.last_request.json() == {
        "useCachedSqlResults": True,
        "dryRun": False,
        "updatePublishedResults": False,
    }


def test_run_project_with_new_params(requests_mock):
    requests_mock.post(
        "https://testurl/api/v1/project/abc-123/run",
        headers={"Content-Type": "application/json"},
        json={"data": "mocked response"},
    )

    hex = HexResource(api_key="abc", base_url="https://testurl/")
    response = hex.run_project(
        "abc-123",
        dry_run=True,
        update_published_results=True,
        use_cached_sql=False,
    )
    assert response == {"data": "mocked response"}
    assert requests_mock.last_request.json() == {
        "useCachedSqlResults": False,
        "dryRun": True,
        "updatePublishedResults": True,
    }


def test_run_project_with_view_id(requests_mock):
    requests_mock.post(
        "https://testurl/api/v1/project/abc-123/run",
        headers={"Content-Type": "application/json"},
        json={"data": "mocked response"},
    )

    hex = HexResource(api_key="abc", base_url="https://testurl/")
    response = hex.run_project(
        "abc-123",
        view_id="view-123",
    )
    assert response == {"data": "mocked response"}
    assert requests_mock.last_request.json() == {
        "useCachedSqlResults": True,
        "dryRun": False,
        "updatePublishedResults": False,
        "viewId": "view-123",
    }


def test_run_project_inputs_and_view_id_raises(requests_mock):
    hex = HexResource(api_key="abc", base_url="https://testurl/")

    try:
        hex.run_project(
            "abc-123",
            inputs={"param": "value"},
            view_id="view-123",
        )
        assert False, "Expected ValueError"
    except ValueError as e:
        assert str(e) == "Cannot specify both inputs and view_id"


def test_run_project_with_notifications(requests_mock):
    requests_mock.post(
        "https://testurl/api/v1/project/abc-123/run",
        headers={"Content-Type": "application/json"},
        json={"data": "mocked response"},
    )

    notifications = [
        {
            "type": "slack",
            "includeSuccessScreenshot": True,
            "slackChannelIds": ["C123456"],
            "userIds": ["U123"],
            "groupIds": ["G456"],
        }
    ]

    hex = HexResource(api_key="abc", base_url="https://testurl/")
    response = hex.run_project(
        "abc-123",
        notifications=notifications,
    )
    assert response == {"data": "mocked response"}
    assert requests_mock.last_request.json() == {
        "useCachedSqlResults": True,
        "dryRun": False,
        "updatePublishedResults": False,
        "notifications": notifications,
    }

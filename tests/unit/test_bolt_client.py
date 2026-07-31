"""Behaviour tests for BoltClient.

These pin down both halves of each method — the request that goes on the wire and
the model that comes back out — so the response-mapping refactor can be verified
as behaviour-preserving rather than eyeballed.
"""

import json

import pytest
import responses
from tests.unit.constants import API_ENDPOINT, WORKSPACE_TOKEN

from paradime.apis.bolt.types import BoltLogStream, BoltRunState
from paradime.client.paradime_client import Paradime


@pytest.fixture
def paradime(monkeypatch):
    """A Paradime client with the PyPI version check disabled."""
    monkeypatch.setenv("PARADIME_DISABLE_VERSION_CHECK", "1")
    return Paradime(api_secret=WORKSPACE_TOKEN, api_endpoint=API_ENDPOINT)


def _gql(data):
    responses.add(
        responses.POST,
        API_ENDPOINT,
        json={"data": data},
        status=200,
    )


def _sent_variables(call_index=0):
    return json.loads(responses.calls[call_index].request.body)["variables"]


SCHEDULE_JSON = {
    "name": "daily",
    "slug": "daily-abc",
    "schedule": "0 9 * * *",
    "owner": "someone@paradime.io",
    "lastRunAt": "2026-07-30T09:00:00Z",
    "lastRunState": "SUCCESS",
    "nextRunAt": "2026-07-31T09:00:00Z",
    "id": 1,
    "uuid": "uuid-1",
    "source": "console",
    "suspended": False,
    "turboCi": None,
    "deferredSchedule": None,
    "commands": ["dbt run"],
    "gitBranch": "main",
    "slackOn": ["failed"],
    "slackNotify": ["#alerts"],
    "emailOn": ["failed"],
    "emailNotify": ["someone@paradime.io"],
    "notifications": None,
}


class TestListSchedules:
    @responses.activate
    def test_parses_schedule_fields(self, paradime):
        _gql({"listBoltSchedules": {"schedules": [SCHEDULE_JSON], "totalCount": 1}})

        result = paradime.bolt.list_schedules()

        assert result.total_count == 1
        schedule = result.schedules[0]
        assert schedule.name == "daily"
        assert schedule.slug == "daily-abc"
        assert schedule.last_run_at == "2026-07-30T09:00:00Z"
        assert schedule.next_run_at == "2026-07-31T09:00:00Z"
        assert schedule.git_branch == "main"
        assert schedule.email_notify == ["someone@paradime.io"]
        assert schedule.turbo_ci is None
        assert schedule.deferred_schedule is None

    @responses.activate
    def test_parses_nested_deferred_schedule(self, paradime):
        schedule = dict(
            SCHEDULE_JSON,
            turboCi={
                "enabled": True,
                "deferredScheduleName": "prod",
                "deferredScheduleSlug": "prod-xyz",
                "successfulRunOnly": True,
            },
        )
        _gql({"listBoltSchedules": {"schedules": [schedule], "totalCount": 1}})

        turbo_ci = paradime.bolt.list_schedules().schedules[0].turbo_ci

        assert turbo_ci is not None
        assert turbo_ci.enabled is True
        assert turbo_ci.deferred_schedule_name == "prod"
        assert turbo_ci.deferred_schedule_slug == "prod-xyz"
        assert turbo_ci.successful_run_only is True

    @responses.activate
    def test_parses_notifications(self, paradime):
        schedule = dict(
            SCHEDULE_JSON,
            notifications={
                "emailNotifications": [
                    {
                        "channel": "someone@paradime.io",
                        "events": ["failed"],
                        "templateSlug": "tmpl-1",
                        "templateName": "Failure",
                    }
                ],
                "slackNotifications": None,
                "msTeamsNotifications": None,
            },
        )
        _gql({"listBoltSchedules": {"schedules": [schedule], "totalCount": 1}})

        notifications = paradime.bolt.list_schedules().schedules[0].notifications

        assert notifications is not None
        assert notifications.slack_notifications is None
        assert notifications.email_notifications is not None
        assert notifications.email_notifications[0].template_slug == "tmpl-1"
        assert notifications.email_notifications[0].template_name == "Failure"

    @responses.activate
    def test_default_pagination_variables(self, paradime):
        _gql({"listBoltSchedules": {"schedules": [], "totalCount": 0}})

        paradime.bolt.list_schedules()

        assert _sent_variables() == {
            "offset": 0,
            "limit": 100,
            "showInactive": False,
            "filter": None,
        }

    @responses.activate
    @pytest.mark.parametrize(
        "suspended,expected_filter",
        [(None, None), (True, {"suspended": True}), (False, {"suspended": False})],
    )
    def test_suspended_filter_is_only_sent_when_set(self, paradime, suspended, expected_filter):
        _gql({"listBoltSchedules": {"schedules": [], "totalCount": 0}})

        paradime.bolt.list_schedules(suspended=suspended)

        assert _sent_variables()["filter"] == expected_filter


class TestListRuns:
    @responses.activate
    def test_parses_run_and_git_info(self, paradime):
        _gql(
            {
                "listBoltRuns": {
                    "ok": True,
                    "runs": [
                        {
                            "id": 42,
                            "state": "SUCCESS",
                            "actor": "someone",
                            "actorEmail": "someone@paradime.io",
                            "startDttm": "2026-07-30T09:00:00Z",
                            "endDttm": "2026-07-30T09:05:00Z",
                            "parentScheduleRunId": 41,
                            "gitInfo": {
                                "branch": "main",
                                "commitHash": "abc123",
                                "pullRequestId": "7",
                            },
                        }
                    ],
                }
            }
        )

        result = paradime.bolt.list_runs(slug="daily-abc")

        assert result.ok is True
        run = result.runs[0]
        assert run.id == 42
        assert run.actor_email == "someone@paradime.io"
        assert run.parent_schedule_run_id == 41
        assert run.start_dttm == "2026-07-30T09:00:00Z"
        assert run.end_dttm == "2026-07-30T09:05:00Z"
        assert run.git_info.commit_hash == "abc123"
        assert run.git_info.pull_request_id == "7"

    @responses.activate
    def test_tolerates_missing_optional_fields(self, paradime):
        _gql(
            {
                "listBoltRuns": {
                    "ok": True,
                    "runs": [
                        {
                            "id": 42,
                            "state": "RUNNING",
                            "actor": "someone",
                            "startDttm": "2026-07-30T09:00:00Z",
                            "gitInfo": {},
                        }
                    ],
                }
            }
        )

        run = paradime.bolt.list_runs(slug="daily-abc").runs[0]

        assert run.actor_email is None
        assert run.end_dttm is None
        assert run.parent_schedule_run_id is None
        assert run.git_info.branch is None

    @pytest.mark.parametrize(
        "kwargs,message",
        [
            ({"offset": -1}, "offset must be >= 0"),
            ({"limit": 0}, "limit must be between 1 and 1000"),
            ({"limit": 1001}, "limit must be between 1 and 1000"),
        ],
    )
    def test_rejects_invalid_pagination(self, paradime, kwargs, message):
        with pytest.raises(ValueError, match=message):
            paradime.bolt.list_runs(slug="daily-abc", **kwargs)


class TestSlugResolution:
    @responses.activate
    def test_slug_is_sent_on_the_wire(self, paradime):
        _gql({"listBoltRuns": {"ok": True, "runs": []}})

        paradime.bolt.list_runs(slug="daily-abc")

        assert _sent_variables()["slug"] == "daily-abc"

    @responses.activate
    def test_schedule_name_is_accepted_but_warns(self, paradime):
        _gql({"listBoltRuns": {"ok": True, "runs": []}})

        with pytest.warns(DeprecationWarning, match="use `slug=` instead"):
            paradime.bolt.list_runs(schedule_name="daily-abc")

        # The deprecated alias still goes on the wire as `slug`.
        assert _sent_variables()["slug"] == "daily-abc"

    def test_rejects_both_slug_and_schedule_name(self, paradime):
        with pytest.raises(ValueError, match="exactly one of"):
            paradime.bolt.list_runs(slug="a", schedule_name="b")

    def test_rejects_neither_slug_nor_schedule_name(self, paradime):
        with pytest.raises(ValueError, match="exactly one of"):
            paradime.bolt.list_runs()


class TestGetRunStatus:
    @responses.activate
    def test_returns_parsed_state(self, paradime):
        _gql({"boltRunStatus": {"state": "SUCCESS"}})

        assert paradime.bolt.get_run_status(1) == BoltRunState.SUCCESS

    @responses.activate
    def test_coerces_run_id_to_int(self, paradime):
        _gql({"boltRunStatus": {"state": "RUNNING"}})

        paradime.bolt.get_run_status(7)

        assert _sent_variables()["runId"] == 7


class TestStreamCommandLogs:
    @responses.activate
    def test_yields_lines_across_polls_until_finished(self, paradime):
        _gql(
            {
                "boltCommandLogs": {
                    "cursor": "1:0",
                    "finished": False,
                    "lines": [{"stream": "STDOUT", "line": "line one"}],
                }
            }
        )
        _gql(
            {
                "boltCommandLogs": {
                    "cursor": "2:0",
                    "finished": True,
                    "lines": [
                        {"stream": "STDOUT", "line": "line two"},
                        {"stream": "STDERR", "line": "oh no"},
                    ],
                }
            }
        )

        lines = list(paradime.bolt.stream_command_logs(1, poll_interval=0))

        assert [line.line for line in lines] == ["line one", "line two", "oh no"]
        assert lines[2].stream is BoltLogStream.STDERR
        # The second poll must resume from the cursor the first poll returned.
        assert _sent_variables(1)["cursor"] == "1:0"

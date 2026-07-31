"""Tests for the lazy metadata client, auto-paging iterators, and poll_until."""

import subprocess
import sys

import pytest
import responses
from tests.unit.constants import API_ENDPOINT, WORKSPACE_TOKEN

from paradime.client.paradime_client import Paradime
from paradime.tools.polling import poll_until


@pytest.fixture
def paradime():
    return Paradime(api_secret=WORKSPACE_TOKEN, api_endpoint=API_ENDPOINT)


def _gql(data):
    responses.add(responses.POST, API_ENDPOINT, json={"data": data}, status=200)


class TestPublicExports:
    def test_client_and_exceptions_are_importable_from_the_package_root(self):
        from paradime import (  # noqa: F401
            BoltRunState,
            Paradime,
            ParadimeAPIException,
            ParadimeAuthException,
            ParadimeException,
            UserAccountType,
        )

    def test_importing_paradime_does_not_pull_in_metadata_dependencies(self):
        """The metadata extra (duckdb/polars/pyarrow) must stay off the import path.

        Run in a subprocess: asserting on a clean sys.modules requires an
        interpreter that has not already imported paradime.
        """
        script = (
            "import sys; import paradime; "
            "heavy = [m for m in ('duckdb', 'polars', 'pyarrow') if m in sys.modules]; "
            "print(','.join(heavy))"
        )
        result = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, check=True
        )

        assert result.stdout.strip() == "", f"eagerly imported: {result.stdout.strip()}"


class TestVersionCheck:
    def test_constructor_does_not_check_pypi_by_default(self, monkeypatch):
        called = []
        monkeypatch.setattr(
            "paradime.client.paradime_client.check_for_new_version",
            lambda: called.append(True),
        )

        Paradime(api_secret=WORKSPACE_TOKEN, api_endpoint=API_ENDPOINT)

        assert called == []

    def test_opt_in_checks_pypi(self, monkeypatch):
        called = []
        monkeypatch.setattr(
            "paradime.client.paradime_client.check_for_new_version",
            lambda: called.append(True),
        )

        Paradime(
            api_secret=WORKSPACE_TOKEN,
            api_endpoint=API_ENDPOINT,
            check_for_updates=True,
        )

        assert called == [True]


class TestLazyMetadata:
    def test_metadata_is_not_built_during_construction(self, paradime):
        assert paradime._metadata is None

    def test_metadata_is_cached_after_first_access(self, paradime):
        pytest.importorskip("duckdb", reason="metadata extra not installed")

        first = paradime.metadata

        assert paradime.metadata is first

    def test_missing_extra_raises_an_actionable_error(self, paradime, monkeypatch):
        real_import = __import__

        def fake_import(name, *args, **kwargs):
            if name.startswith("paradime.apis.metadata"):
                raise ImportError("No module named 'duckdb'")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr("builtins.__import__", fake_import)

        with pytest.raises(ImportError, match=r"paradime-io\[metadata\]"):
            paradime.metadata


class TestIterSchedules:
    @responses.activate
    def test_pages_until_total_count_is_reached(self, paradime):
        def page(names, total):
            return {
                "listBoltSchedules": {
                    "schedules": [
                        {
                            "name": n,
                            "slug": n,
                            "schedule": "0 9 * * *",
                            "owner": None,
                            "lastRunAt": None,
                            "lastRunState": None,
                            "nextRunAt": None,
                            "id": i,
                            "uuid": f"u-{i}",
                            "source": "console",
                            "suspended": False,
                            "turboCi": None,
                            "deferredSchedule": None,
                            "commands": [],
                            "gitBranch": None,
                            "slackOn": None,
                            "slackNotify": None,
                            "emailOn": None,
                            "emailNotify": None,
                            "notifications": None,
                        }
                        for i, n in enumerate(names)
                    ],
                    "totalCount": total,
                }
            }

        _gql(page(["a", "b"], 3))
        _gql(page(["c"], 3))

        names = [s.name for s in paradime.bolt.iter_schedules(page_size=2)]

        assert names == ["a", "b", "c"]
        assert len(responses.calls) == 2

    @responses.activate
    def test_stops_on_an_empty_page_even_if_total_count_disagrees(self, paradime):
        """A total_count that overstates the rows must not spin forever."""
        _gql({"listBoltSchedules": {"schedules": [], "totalCount": 99}})

        assert list(paradime.bolt.iter_schedules()) == []
        assert len(responses.calls) == 1


class TestIterRuns:
    @responses.activate
    def test_stops_on_the_first_short_page(self, paradime):
        def page(ids):
            return {
                "listBoltRuns": {
                    "ok": True,
                    "runs": [
                        {
                            "id": i,
                            "state": "SUCCESS",
                            "actor": "someone",
                            "startDttm": "2026-07-30T09:00:00Z",
                            "gitInfo": {},
                        }
                        for i in ids
                    ],
                }
            }

        _gql(page([1, 2]))
        _gql(page([3]))

        ids = [r.id for r in paradime.bolt.iter_runs(slug="daily", page_size=2)]

        assert ids == [1, 2, 3]
        assert len(responses.calls) == 2

    def test_requires_exactly_one_of_slug_or_schedule_name(self, paradime):
        with pytest.raises(ValueError, match="exactly one of"):
            list(paradime.bolt.iter_runs())


class TestPollUntil:
    def test_returns_immediately_when_already_done(self):
        calls = []

        result = poll_until(
            lambda: calls.append(1) or "done",
            lambda s: s == "done",
            timeout=10,
            interval=0,
        )

        assert result == "done"
        assert len(calls) == 1

    def test_polls_until_the_condition_holds(self):
        states = iter(["pending", "pending", "done"])

        result = poll_until(lambda: next(states), lambda s: s == "done", timeout=10, interval=0)

        assert result == "done"

    def test_raises_timeout_with_the_last_state(self, monkeypatch):
        clock = iter([0.0, 0.0, 100.0, 100.0])
        monkeypatch.setattr("paradime.tools.polling.time.monotonic", lambda: next(clock))

        with pytest.raises(TimeoutError, match="stuck"):
            poll_until(
                lambda: "pending",
                lambda s: False,
                timeout=1,
                interval=0,
                timeout_message=lambda s: f"stuck in {s}",
            )

    def test_on_poll_sees_only_non_terminal_states(self):
        states = iter(["a", "b", "done"])
        seen = []

        poll_until(
            lambda: next(states),
            lambda s: s == "done",
            timeout=10,
            interval=0,
            on_poll=seen.append,
        )

        assert seen == ["a", "b"]

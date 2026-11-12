from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Union

import pytest
from click.testing import CliRunner

from paradime.apis.catalog.client import CatalogClient
from paradime.apis.catalog.types import (
    CatalogRefresh,
    CatalogRefreshError,
    CatalogRefreshProgress,
    CatalogRefreshRequest,
    CatalogRefreshStatus,
    CatalogRefreshStep,
    CatalogRefreshStepStatus,
)
from paradime.cli import catalog as catalog_cli
from paradime.cli.catalog import RefreshFollower, format_duration, format_progress, parse_timeout
from paradime.client.api_exception import ParadimeAPIException

T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

Poll = Union[CatalogRefresh, Exception, None]

DOCS = "docs_generate.dbt"
PARSE = "docs_generate.parse"
BUILD = "catalog.build"
DATAHUB = "datahub.push"
LABELS = {
    DOCS: "dbt docs generate",
    PARSE: "Read dbt artifacts",
    BUILD: "Catalog build",
    DATAHUB: "DataHub push",
}


def _iso(seconds: Optional[float]) -> Optional[str]:
    return None if seconds is None else (T0 + timedelta(seconds=seconds)).isoformat()


def _step(
    kind: str,
    status: str,
    *,
    blocking: bool = True,
    started: Optional[float] = None,
    finished: Optional[float] = None,
    progress: Optional[CatalogRefreshProgress] = None,
    error: Optional[CatalogRefreshError] = None,
) -> CatalogRefreshStep:
    return CatalogRefreshStep(
        kind=kind,
        label=LABELS.get(kind),
        status=CatalogRefreshStepStatus(status),
        blocking=blocking,
        started_at=_iso(started),
        finished_at=_iso(finished),
        progress=progress,
        error=error,
    )


def _refresh(status: str, *steps: CatalogRefreshStep, error: Any = None) -> CatalogRefresh:
    return CatalogRefresh(
        token="refresh-1",
        status=CatalogRefreshStatus(status),
        finished=status != "RUNNING",
        steps=list(steps),
        error=error,
    )


class FakeClock:
    def __init__(self) -> None:
        self.t = 0.0

    def monotonic(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.t += seconds

    def now(self) -> datetime:
        return T0 + timedelta(seconds=self.t)


def _follow(
    polls: List[Poll],
    *,
    timeout: float = 7200,
    allow_integration_failures: bool = False,
) -> "tuple[int, List[str], List[float]]":
    """Run the follower over scripted poll results; the last one repeats."""

    clock = FakeClock()
    lines: List[str] = []
    sleeps: List[float] = []
    calls = {"n": 0}

    def get_refresh(token: str) -> Optional[CatalogRefresh]:
        assert token == "refresh-1"
        result = polls[min(calls["n"], len(polls) - 1)]
        calls["n"] += 1
        if isinstance(result, Exception):
            raise result
        return result

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock.sleep(seconds)

    code = RefreshFollower(
        get_refresh=get_refresh,
        token="refresh-1",
        timeout=timeout,
        allow_integration_failures=allow_integration_failures,
        echo=lines.append,
        sleep=sleep,
        monotonic=clock.monotonic,
        now=clock.now,
    ).run()
    return code, [" ".join(line.split()) for line in lines], sleeps


def _progress(done: int, total: int) -> CatalogRefreshProgress:
    return CatalogRefreshProgress(
        stage="write_assets", stage_label="Write assets", done=done, total=total, unit="assets"
    )


class TestFollowSuccess:
    def test_logs_each_change_once_and_exits_zero(self) -> None:
        polls: List[Poll] = [
            _refresh("RUNNING", _step(DOCS, "RUNNING", started=0), _step(BUILD, "PENDING")),
            _refresh("RUNNING", _step(DOCS, "RUNNING", started=0), _step(BUILD, "PENDING")),
            _refresh(
                "RUNNING",
                _step(DOCS, "SUCCEEDED", started=0, finished=252),
                _step(BUILD, "RUNNING", started=252, progress=_progress(18400, 41200)),
            ),
            _refresh(
                "RUNNING",
                _step(DOCS, "SUCCEEDED", started=0, finished=252),
                _step(BUILD, "RUNNING", started=252, progress=_progress(30000, 41200)),
            ),
            _refresh(
                "SUCCEEDED",
                _step(DOCS, "SUCCEEDED", started=0, finished=252),
                _step(BUILD, "SUCCEEDED", started=252, finished=315),
            ),
        ]
        code, lines, sleeps = _follow(polls)

        assert code == 0
        assert lines == [
            "[dbt docs generate] started",
            "[dbt docs generate] done in 4m12s",
            "[Catalog build] started, Write assets 18,400 / 41,200 assets",
            "[Catalog build] running: Write assets 30,000 / 41,200 assets",
            "[Catalog build] done in 1m03s",
            "[catalog refresh] done in 20s, 2 of 2 steps succeeded",
        ]
        assert sleeps == [5.0] * 4

    def test_heartbeat_once_a_minute_while_nothing_changes(self) -> None:
        running = _refresh("RUNNING", _step(DOCS, "RUNNING", started=0))
        done = _refresh("SUCCEEDED", _step(DOCS, "SUCCEEDED", started=0, finished=200))
        code, lines, _ = _follow([running] * 40 + [done])

        assert code == 0
        heartbeats = [line for line in lines if "still running" in line]
        assert heartbeats == [
            "[dbt docs generate] still running (1m00s)",
            "[dbt docs generate] still running (2m00s)",
            "[dbt docs generate] still running (3m00s)",
        ]

    def test_waits_while_an_integration_push_keeps_the_refresh_running(self) -> None:
        polls: List[Poll] = [
            _refresh(
                "RUNNING",
                _step(BUILD, "SUCCEEDED", started=0, finished=10),
                _step(DATAHUB, "RUNNING", blocking=False, started=10),
            ),
            _refresh(
                "SUCCEEDED",
                _step(BUILD, "SUCCEEDED", started=0, finished=10),
                _step(DATAHUB, "SUCCEEDED", blocking=False, started=10, finished=32),
            ),
        ]
        code, lines, _ = _follow(polls)

        assert code == 0
        assert lines[-2:] == [
            "[DataHub push] done in 22s",
            "[catalog refresh] done in 5s, 2 of 2 steps succeeded",
        ]

    def test_a_step_that_ends_between_two_polls_logs_only_its_end(self) -> None:
        code, lines, _ = _follow(
            [_refresh("SUCCEEDED", _step(PARSE, "SUCCEEDED", started=0, finished=3))]
        )

        assert code == 0
        assert lines[0] == "[Read dbt artifacts] done in 3s"


class TestFollowFailure:
    def test_failed_docs_generate_prints_the_dbt_output_tail(self) -> None:
        output = "\n".join(f"line {n}" for n in range(100))
        polls: List[Poll] = [
            _refresh(
                "FAILED",
                _step(
                    DOCS,
                    "FAILED",
                    started=0,
                    finished=30,
                    error=CatalogRefreshError(
                        code="DOCS_GENERATE_FAILED",
                        message="dbt docs generate failed",
                        detail=output,
                    ),
                ),
                _step(BUILD, "SKIPPED"),
            )
        ]
        code, lines, _ = _follow(polls, allow_integration_failures=True)

        assert code == 1
        assert lines[0] == "[dbt docs generate] FAILED: dbt docs generate failed"
        assert lines[1] == "line 60" and lines[40] == "line 99"
        assert lines[-2] == "[Catalog build] skipped"
        assert lines[-1] == "[catalog refresh] FAILED: 1 of 2 steps failed (dbt docs generate)"

    def test_integration_failure_fails_without_the_flag(self) -> None:
        polls: List[Poll] = [
            _refresh(
                "SUCCEEDED",
                _step(BUILD, "SUCCEEDED", started=0, finished=10),
                _step(
                    DATAHUB,
                    "FAILED",
                    blocking=False,
                    error=CatalogRefreshError(
                        code="DATAHUB_PUSH_FAILED", message="401 Unauthorized"
                    ),
                ),
            )
        ]
        code, lines, _ = _follow(polls)

        assert code == 1
        assert "[DataHub push] FAILED: 401 Unauthorized" in lines
        assert lines[-1] == "[catalog refresh] FAILED: 1 of 2 steps failed (DataHub push)"

    def test_integration_failure_is_a_warning_with_the_flag(self) -> None:
        polls: List[Poll] = [
            _refresh(
                "SUCCEEDED",
                _step(BUILD, "SUCCEEDED", started=0, finished=10),
                _step(
                    DATAHUB,
                    "FAILED",
                    blocking=False,
                    error=CatalogRefreshError(
                        code="DATAHUB_PUSH_FAILED", message="401 Unauthorized"
                    ),
                ),
            )
        ]
        code, lines, _ = _follow(polls, allow_integration_failures=True)

        assert code == 0
        assert "[DataHub push] FAILED (allowed): 401 Unauthorized" in lines
        assert lines[-1] == (
            "[catalog refresh] done in 0s, 1 of 2 steps succeeded; 1 integration step(s) "
            "failed and were allowed (DataHub push)"
        )

    def test_the_flag_never_allows_a_blocking_failure(self) -> None:
        polls: List[Poll] = [
            _refresh("FAILED", _step(BUILD, "FAILED", error=None)),
        ]
        code, lines, _ = _follow(polls, allow_integration_failures=True)

        assert code == 1
        assert lines == [
            "[Catalog build] FAILED: no error message",
            "[catalog refresh] FAILED: 1 of 1 steps failed (Catalog build)",
        ]

    def test_a_lost_step_fails(self) -> None:
        code, lines, _ = _follow([_refresh("LOST", _step(BUILD, "LOST", started=0))])

        assert code == 1
        assert lines[0] == "[Catalog build] FAILED: the step stopped reporting progress"

    def test_a_failed_refresh_without_failed_steps_fails_with_its_error(self) -> None:
        error = CatalogRefreshError(code="INPUT_MISSING", message="no production environment")
        code, lines, _ = _follow([_refresh("FAILED", error=error)])

        assert code == 1
        assert lines == ["[catalog refresh] FAILED: no production environment"]

    def test_a_skipped_refresh_fails(self) -> None:
        error = CatalogRefreshError(code="SKIPPED", message="no warehouse connection")
        code, lines, _ = _follow([_refresh("SKIPPED", error=error)])

        assert code == 1
        assert lines == [
            "[catalog refresh] FAILED: the refresh was skipped, nothing was refreshed: "
            "no warehouse connection"
        ]


class TestFollowTimeout:
    def test_names_the_steps_still_running_and_exits_non_zero(self) -> None:
        running = _refresh(
            "RUNNING",
            _step(DOCS, "SUCCEEDED", started=0, finished=10),
            _step(BUILD, "RUNNING", started=10),
            _step(DATAHUB, "PENDING", blocking=False),
        )
        code, lines, sleeps = _follow([running], timeout=12)

        assert code == 1
        assert sleeps == [5.0, 5.0, 2.0]
        assert lines[-1] == (
            "[catalog refresh] FAILED: timed out after 12s; not finished: Catalog build, "
            "DataHub push. The refresh continues in the background."
        )


class TestFollowTransientErrors:
    def test_retries_with_backoff_and_recovers(self) -> None:
        done = _refresh("SUCCEEDED", _step(BUILD, "SUCCEEDED", started=0, finished=1))
        polls: List[Poll] = [
            ParadimeAPIException("Error: 502 - Bad Gateway"),
            ParadimeAPIException("Error: 502 - Bad Gateway"),
            None,
            done,
        ]
        code, lines, sleeps = _follow(polls)

        assert code == 0
        assert sleeps == [5.0, 10.0, 20.0]
        assert lines[0] == (
            "[catalog refresh] could not read the refresh state (Error: 502 - Bad Gateway), "
            "retrying in 5s"
        )
        assert lines[2] == (
            "[catalog refresh] could not read the refresh state (no catalog refresh with id "
            "refresh-1), retrying in 20s"
        )
        assert lines[-1].startswith("[catalog refresh] done in")

    def test_gives_up_when_reads_keep_failing(self) -> None:
        code, lines, sleeps = _follow([ParadimeAPIException("Error: 503 - Unavailable")])

        assert code == 1
        assert max(sleeps) == 60.0
        assert lines[-1].startswith(
            "[catalog refresh] FAILED: could not read the refresh state for"
        )


class TestParsing:
    @pytest.mark.parametrize(
        "value, seconds",
        [("90", 90), ("90s", 90), ("45m", 2700), ("2h", 7200), ("1h30m", 5400)],
    )
    def test_parse_timeout(self, value: str, seconds: float) -> None:
        assert parse_timeout(value) == seconds

    @pytest.mark.parametrize("value", ["", "0", "2 hours", "h", "-5m", "1x"])
    def test_parse_timeout_rejects(self, value: str) -> None:
        with pytest.raises(ValueError):
            parse_timeout(value)

    def test_format_progress_without_a_measured_total(self) -> None:
        unmeasured = CatalogRefreshProgress(
            stage="push", stage_label="Push", done=120, total=0, unit="documents"
        )
        starting = CatalogRefreshProgress(
            stage="load", stage_label="Load", done=0, total=0, unit="bytes"
        )

        assert format_progress(unmeasured) == "Push 120 documents"
        assert format_progress(starting) == "Load"
        assert format_progress(_progress(18400, 41200)) == "Write assets 18,400 / 41,200 assets"

    def test_format_duration(self) -> None:
        assert [format_duration(s) for s in (0, 22, 63, 252, 3720)] == [
            "0s",
            "22s",
            "1m03s",
            "4m12s",
            "1h02m",
        ]


class FakeAPI:
    def __init__(self, responses: List[Dict[str, Any]]) -> None:
        self.responses = responses
        self.calls: List[Dict[str, Any]] = []

    def _call_gql(self, query: str, variables: Dict[str, Any] = {}) -> Dict[str, Any]:
        self.calls.append({"query": query, "variables": variables})
        return self.responses[len(self.calls) - 1]


class TestCatalogClient:
    def test_refresh_returns_the_token_and_whether_it_joined(self) -> None:
        api = FakeAPI([{"refreshCatalog": {"ok": True, "token": "abc", "joined": True}}])
        result = CatalogClient(api).refresh()  # type: ignore[arg-type]

        assert result == CatalogRefreshRequest(token="abc", joined=True)

    def test_get_refresh_parses_steps(self) -> None:
        api = FakeAPI(
            [
                {
                    "catalogRefresh": {
                        "token": "abc",
                        "status": "RUNNING",
                        "startedAt": "2026-01-01T12:00:00+00:00",
                        "finishedAt": None,
                        "error": None,
                        "finished": False,
                        "steps": [
                            {
                                "kind": BUILD,
                                "label": "Catalog build",
                                "status": "RUNNING",
                                "blocking": True,
                                "startedAt": "2026-01-01T12:00:00+00:00",
                                "finishedAt": None,
                                "progress": {
                                    "stage": "write_assets",
                                    "stageLabel": "Write assets",
                                    "done": 5,
                                    "total": 10,
                                    "unit": "assets",
                                },
                                "error": None,
                            }
                        ],
                    }
                },
                {"catalogRefresh": None},
            ]
        )
        client = CatalogClient(api)  # type: ignore[arg-type]
        refresh = client.get_refresh("abc")

        assert api.calls[0]["variables"] == {"token": "abc"}
        assert refresh is not None
        assert refresh.status is CatalogRefreshStatus.RUNNING
        assert refresh.finished is False
        assert refresh.steps[0].label == "Catalog build"
        assert refresh.steps[0].progress == _progress(5, 10)
        assert client.get_refresh("missing") is None


class FakeCatalog:
    def __init__(self, request: CatalogRefreshRequest, refresh: CatalogRefresh) -> None:
        self.request = request
        self._refresh = refresh
        self.polls = 0

    def refresh(self) -> CatalogRefreshRequest:
        return self.request

    def get_refresh(self, token: str) -> CatalogRefresh:
        self.polls += 1
        return self._refresh


class FakeCLIClient:
    def __init__(self, catalog: FakeCatalog) -> None:
        self.catalog = catalog


def _invoke(monkeypatch: pytest.MonkeyPatch, catalog: FakeCatalog, *args: str) -> Any:
    monkeypatch.setattr(catalog_cli, "get_cli_client_or_exit", lambda: FakeCLIClient(catalog))
    monkeypatch.setattr(catalog_cli.time, "sleep", lambda _: None)
    return CliRunner().invoke(catalog_cli.refresh, list(args))


class TestRefreshCommand:
    failed = _refresh("FAILED", _step(BUILD, "FAILED"))

    def test_no_wait_prints_the_id_and_exits_zero(self, monkeypatch: pytest.MonkeyPatch) -> None:
        catalog = FakeCatalog(CatalogRefreshRequest(token="refresh-1"), self.failed)
        result = _invoke(monkeypatch, catalog, "--no-wait")

        assert result.exit_code == 0
        assert " ".join(result.output.split()) == "[catalog refresh] started, refresh id refresh-1"
        assert catalog.polls == 0

    def test_waits_by_default_and_fails_with_the_refresh(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        catalog = FakeCatalog(CatalogRefreshRequest(token="refresh-1"), self.failed)
        result = _invoke(monkeypatch, catalog)

        assert result.exit_code == 1
        assert "[Catalog build] FAILED" in " ".join(result.output.split())
        assert catalog.polls == 1

    def test_says_when_it_joined_a_running_refresh(self, monkeypatch: pytest.MonkeyPatch) -> None:
        done = _refresh("SUCCEEDED", _step(BUILD, "SUCCEEDED", started=0, finished=1))
        catalog = FakeCatalog(CatalogRefreshRequest(token="refresh-1", joined=True), done)
        result = _invoke(monkeypatch, catalog)

        assert result.exit_code == 0
        first = " ".join(result.output.splitlines()[0].split())
        assert first == "[catalog refresh] already running, following refresh id refresh-1"

    def test_rejects_a_bad_timeout(self, monkeypatch: pytest.MonkeyPatch) -> None:
        catalog = FakeCatalog(CatalogRefreshRequest(token="refresh-1"), self.failed)
        result = _invoke(monkeypatch, catalog, "--timeout", "soon")

        assert result.exit_code == 2
        assert "invalid duration" in result.output

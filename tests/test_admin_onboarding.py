import importlib.util
import io
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, Iterator, List, Optional, Tuple

import pytest
from admin_onboarding_stub import (
    BOOTSTRAP_UID,
    LEFTOVER_UID,
    NEW_UID,
    StubAdminApi,
    _workspace,
    start_stub,
)

EXAMPLE_DIR = Path(__file__).resolve().parents[1] / "examples" / "admin_onboarding"
EXAMPLE_CONFIG = EXAMPLE_DIR / "config.example.yml"
API_SECRET = "prdm_cmp_onboarding-test-secret"
GITHUB_TOKEN = "ghp_onboarding-test-token"
PRIVATE_KEY = "-----BEGIN PRIVATE KEY-----onboarding-test"
WEBHOOK_URL = "https://acme.webhook.office.com/webhookb2/onboarding-test-secret"
ENVIRON = {
    "ONBOARDING_SNOWFLAKE_PRIVATE_KEY": PRIVATE_KEY,
    "ONBOARDING_TEAMS_WEBHOOK_URL": WEBHOOK_URL,
}


def _load_example() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "admin_onboarding_example", EXAMPLE_DIR / "onboard.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


onboard = _load_example()


@pytest.fixture(autouse=True)
def _no_version_check(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PARADIME_DISABLE_VERSION_CHECK", "1")


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        self.now += 0.5
        return self.now


def _runner(phase: int = 2, **kwargs: Any) -> Tuple[Any, io.StringIO]:
    out = io.StringIO()
    return onboard.StepRunner(phase, out=out, clock=Clock(), **kwargs), out


def _passes(detail: str = "ok") -> Any:
    return lambda: onboard.Outcome(detail)


def _raises(error: BaseException) -> Any:
    def action() -> Any:
        raise error

    return action


def test_runner_prints_one_line_per_step_with_status_and_timing() -> None:
    runner, out = _runner()

    runner.run(onboard.Step(1, "createWorkspace", _passes("ws-1")))
    runner.run(onboard.Step(2, "gitDeployKey", _raises(onboard.StepFailed("no key"))))
    runner.run(onboard.Step(3, "connectGitRepository", _raises(onboard.SkipStep("no git block"))))

    lines = out.getvalue().splitlines()
    assert lines[0].split() == ["PASS", "01", "createWorkspace", "0.50s", "ws-1"]
    assert lines[1].split() == ["FAIL", "02", "gitDeployKey", "0.50s", "no", "key"]
    assert lines[2].split()[:4] == ["SKIP", "03", "connectGitRepository", "0.50s"]
    assert runner.failed


def test_runner_reports_an_unexpected_error_with_its_class() -> None:
    runner, out = _runner()

    runner.run(onboard.Step(4, "updateWorkspace", _raises(KeyError("dbt_version"))))

    assert "FAIL  04" in out.getvalue()
    assert "KeyError: 'dbt_version'" in out.getvalue()


def test_runner_skips_a_step_whose_needed_step_did_not_pass() -> None:
    runner, out = _runner()
    called: List[int] = []

    runner.run(onboard.Step(6, "createEnvironment", _raises(onboard.StepFailed("refused"))))
    runner.run(onboard.Step(7, "schedulerConnections", lambda: called.append(7), needs=(6,)))

    assert called == []
    assert "SKIP  07   schedulerConnections" in out.getvalue()
    assert "needs step 6, which did not pass" in out.getvalue()


def test_phase_one_runs_steps_1_to_8_and_13_and_always_the_leftovers_and_cleanup() -> None:
    runner, out = _runner(phase=1)
    called: List[int] = []

    def record(number: int) -> Any:
        return lambda: called.append(number) or onboard.Outcome()

    for number in range(0, onboard.TOTAL_STEPS + 2):
        runner.run(onboard.Step(number, f"step{number}", record(number)))

    assert called == [0, 1, 2, 3, 4, 5, 6, 7, 8, 13, 15]
    assert "SKIP  14   step14" in out.getvalue()
    assert "phase 2 step, not run with --phase 1" in out.getvalue()
    assert runner.summary().endswith("9 passed, 0 failed, 0 skipped of 9 steps; exit criterion met")


def test_summary_names_the_steps_that_did_not_pass() -> None:
    runner, _ = _runner()

    runner.run(onboard.Step(1, "createWorkspace", _passes()))
    runner.run(onboard.Step(9, "agentEnvironment", _raises(onboard.SkipStep("optional"))))
    runner.run(onboard.Step(10, "deployBoltSchedules", _raises(onboard.StepFailed("yaml"))))

    assert runner.summary() == (
        "phase 2: 1 passed, 1 failed, 1 skipped of 3 steps; exit criterion not met (steps 9, 10)"
    )


def test_runner_masks_secrets_in_details_and_notes() -> None:
    runner, out = _runner(secrets={"s3cr3t-value", "abc"})

    runner.run(
        onboard.Step(
            5,
            "devConnection",
            lambda: onboard.Outcome("sent s3cr3t-value", ["note s3cr3t-value"]),
        )
    )

    assert "s3cr3t-value" not in out.getvalue()
    assert out.getvalue().count("****") == 2


@pytest.fixture()
def stub() -> Iterator[Tuple[StubAdminApi, str]]:
    api = StubAdminApi()
    server, url = start_stub(api)
    yield api, url
    server.shutdown()


def _onboarding(url: str, sleeps: Optional[List[float]] = None, **kwargs: Any) -> Any:
    config = onboard.ConfigLoader(EXAMPLE_CONFIG, ENVIRON).load()
    config.poll_interval = 0.01
    return onboard.Onboarding(
        config,
        api_endpoint=f"{url}/graphql",
        api_secret=API_SECRET,
        bootstrap_workspace_uid=BOOTSTRAP_UID,
        github=onboard.GitHubDeployKeys(GITHUB_TOKEN, f"{url}/github"),
        sleep=(sleeps if sleeps is not None else []).append,
        now=datetime(2026, 11, 5, 12, 0, tzinfo=timezone.utc),
        **kwargs,
    )


def _run(onboarding: Any, phase: int = 2, keep: bool = False) -> Tuple[int, str]:
    runner, out = _runner(
        phase,
        secrets=onboarding.config.secrets | {API_SECRET, GITHUB_TOKEN},
        drain_notes=onboarding.drain_notes,
    )
    code = onboard.run_onboarding(onboarding, runner, keep=keep)
    return code, out.getvalue()


def _operations(api: StubAdminApi) -> List[str]:
    return [operation for operation, _ in api.calls]


def test_full_run_against_the_stub_passes_every_step_and_deletes_the_workspace(
    stub: Tuple[StubAdminApi, str],
) -> None:
    api, url = stub
    sleeps: List[float] = []

    code, output = _run(_onboarding(url, sleeps))

    assert code == 0
    lines = output.splitlines()
    statuses = [line[:4] for line in lines if line[6:8].isdigit()]
    assert statuses == ["PASS"] * 14
    assert "exit criterion met" in lines[-1]
    assert api.deleted == [LEFTOVER_UID, NEW_UID]
    assert api.github_keys == {}
    assert api.polls["job-git"] == 2
    assert 0.05 in sleeps
    assert "RATE_LIMITED: retried after 0.05s" in output
    assert "PENDING_USER_ACTION GITHUB_APP (optional)" in output
    assert "PENDING_USER_ACTION Slack" in output
    for secret in (API_SECRET, GITHUB_TOKEN, PRIVATE_KEY, WEBHOOK_URL):
        assert secret not in output


def test_workspace_level_calls_target_the_new_workspace(stub: Tuple[StubAdminApi, str]) -> None:
    api, url = stub

    _run(_onboarding(url))

    created = _operations(api).index("adminCreateWorkspace")
    account_level = {"adminWorkspaces", "adminDeleteWorkspace", "github GET", "github POST"}
    later = [
        (operation, workspace)
        for operation, workspace in api.calls[created + 1 :]
        if operation not in account_level and not operation.startswith("github")
    ]
    assert later and all(workspace == NEW_UID for _, workspace in later)
    assert _operations(api).index("adminDeleteWorkspace") < created


def test_a_failed_step_still_deletes_the_workspace_and_exits_non_zero() -> None:
    api = StubAdminApi(fail_operation="adminSetWarehouseConnection")
    server, url = start_stub(api)
    try:
        code, output = _run(_onboarding(url))
    finally:
        server.shutdown()

    assert code == 1
    assert "FAIL  07   schedulerConnections" in output
    assert "CONNECTION_TEST_FAILED" in output
    assert "SKIP  13   triggerBoltRun" in output
    assert NEW_UID in api.deleted
    assert "PASS  end  deleteWorkspace (cleanup)" in output


def test_an_interrupted_run_still_deletes_the_workspace(stub: Tuple[StubAdminApi, str]) -> None:
    api, url = stub
    onboarding = _onboarding(url)
    onboarding.update_workspace = _raises(KeyboardInterrupt())

    with pytest.raises(KeyboardInterrupt):
        _run(onboarding)

    assert NEW_UID in api.deleted
    assert "adminCreateEnvironment" not in _operations(api)


def test_keep_leaves_the_workspace_for_the_next_run_to_delete(
    stub: Tuple[StubAdminApi, str],
) -> None:
    api, url = stub

    code, output = _run(_onboarding(url), phase=1, keep=True)

    assert code == 0
    assert NEW_UID not in api.deleted
    assert "SKIP  end  deleteWorkspace (cleanup)" in output
    assert "because of --keep" in output

    code, output = _run(_onboarding(url), phase=1)

    assert code == 0
    assert api.deleted == [LEFTOVER_UID, NEW_UID, NEW_UID]


def test_cleanup_finds_the_workspace_by_name_when_its_uid_is_unknown(
    stub: Tuple[StubAdminApi, str],
) -> None:
    api, url = stub
    onboarding = _onboarding(url)
    onboarding.create_workspace()
    onboarding.workspace_uid = None

    outcome = onboarding.clean_up()

    assert NEW_UID in api.deleted
    assert NEW_UID in outcome.detail


class MissingStepApi(StubAdminApi):
    def op_onboardingWorkspaceSetupStatus(self, variables: Dict[str, Any], workspace: Any) -> Any:
        status, reply = super().op_onboardingWorkspaceSetupStatus(variables, workspace)
        steps = reply["data"]["workspaceSetupStatus"]["steps"]
        steps[-1].update(status="MISSING", detail="No Bolt run has finished yet.")
        reply["data"]["workspaceSetupStatus"]["isComplete"] = False
        return status, reply


def test_setup_status_fails_on_a_missing_required_row_not_on_pending_ones() -> None:
    api = MissingStepApi()
    server, url = start_stub(api)
    try:
        code, output = _run(_onboarding(url))
    finally:
        server.shutdown()

    assert code == 1
    assert "FAIL  14   workspaceSetupStatus" in output
    assert "required steps missing: FIRST_BOLT_RUN" in output
    assert "PENDING_USER_ACTION GITHUB_APP (optional)" in output
    assert "MISSING             FIRST_BOLT_RUN: No Bolt run has finished yet." in output


def test_main_refuses_a_workspace_key_before_any_request(capsys: pytest.CaptureFixture) -> None:
    environ = {
        "PARADIME_API_SECRET": "prdm_wsp_workspace-key",
        "PARADIME_API_ENDPOINT": "http://127.0.0.1:1/graphql",
        "PARADIME_WORKSPACE_UID": BOOTSTRAP_UID,
        "PARADIME_ONBOARDING_CONFIG": str(EXAMPLE_CONFIG),
    }

    assert onboard.main([], environ) == 2
    assert "account API key" in capsys.readouterr().err


def test_a_secret_reference_to_an_unset_variable_is_a_config_error() -> None:
    with pytest.raises(onboard.ConfigError, match="ONBOARDING_SNOWFLAKE_PRIVATE_KEY"):
        onboard.ConfigLoader(EXAMPLE_CONFIG, {}).load()


def test_config_reads_variables_from_files_and_keeps_secrets_out_of_argv() -> None:
    config = onboard.ConfigLoader(EXAMPLE_CONFIG, ENVIRON).load()

    assert config.variables == {
        "": {"COMPANY": "acme", "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"},
        "staging": {"DBT_TARGET_SCHEMA": "analytics_staging"},
    }
    assert {PRIVATE_KEY, WEBHOOK_URL} <= config.secrets
    assert "acme" not in config.secrets


def _config_file(tmp_path: Path, old: str, new: str) -> Path:
    for name in ("workspace.env.example", "staging.env.example"):
        (tmp_path / name).write_text((EXAMPLE_DIR / name).read_text())
    config = tmp_path / "config.yml"
    config.write_text(EXAMPLE_CONFIG.read_text().replace(old, new))
    return config


def test_the_sweep_deletes_only_workspace_names_the_script_makes(
    stub: Tuple[StubAdminApi, str],
) -> None:
    api, url = stub
    api.workspaces["ws-prod"] = _workspace("ws-prod", "onboarding-e2e-prod")
    api.workspaces["ws-later"] = _workspace("ws-later", "onboarding-e2e-20261104-020000-copy")
    api.workspaces[BOOTSTRAP_UID]["name"] = "onboarding-e2e-20261101-000000"

    _onboarding(url).remove_leftovers()

    assert api.deleted == [LEFTOVER_UID]
    assert {"ws-prod", "ws-later", BOOTSTRAP_UID} <= set(api.workspaces)


def test_a_prefix_without_the_marker_is_refused_before_any_request(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    config = _config_file(tmp_path, "name_prefix: onboarding-e2e", "name_prefix: analytics")
    environ = {
        **ENVIRON,
        "PARADIME_API_SECRET": API_SECRET,
        "PARADIME_API_ENDPOINT": "http://127.0.0.1:1/graphql",
        "PARADIME_WORKSPACE_UID": BOOTSTRAP_UID,
        "PARADIME_ONBOARDING_CONFIG": str(config),
    }

    assert onboard.main([], environ) == 2
    assert "must start with 'onboarding-'" in capsys.readouterr().err


def test_the_key_sweep_deletes_only_keys_the_script_adds(stub: Tuple[StubAdminApi, str]) -> None:
    api, url = stub
    api.github_keys.update({50: "analytics-ci", 51: "onboarding-e2e-ci"})

    _onboarding(url).remove_leftovers()

    assert api.github_keys == {50: "analytics-ci", 51: "onboarding-e2e-ci"}

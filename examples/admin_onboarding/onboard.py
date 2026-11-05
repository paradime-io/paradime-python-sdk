import argparse
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    TextIO,
    Tuple,
    TypeVar,
)
from urllib.parse import urlparse

import requests
from dotenv import dotenv_values
from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError

from paradime import Paradime
from paradime.apis.admin.connections.types import ConnectionScope, WarehouseAdapterInput
from paradime.apis.admin.environments.types import EnvironmentType
from paradime.apis.admin.exception import AdminApiException
from paradime.apis.admin.git.types import (
    DeployKeyConnectionInput,
    GitProvider,
    GitRepository,
    GitRepositoryState,
)
from paradime.apis.admin.notifications.types import SlackInstallationStatus
from paradime.apis.admin.setup_status.types import SetupStepStatus
from paradime.apis.admin.types import SECRET_INPUT_FIELDS, UNSET, given
from paradime.apis.bolt.types import BoltRunState
from paradime.apis.users.types import UserAccountType
from paradime.client.api_client import COMPANY_API_TOKEN_PREFIX
from paradime.tools.pydantic import ValidationError

T = TypeVar("T")

PHASE_ONE_STEPS = frozenset({1, 2, 3, 4, 5, 6, 7, 8, 13})
TOTAL_STEPS = 14
EXIT_CRITERION_NOT_MET = 3
RUN_REASON = "Admin API onboarding acceptance test"
GITHUB_API_URL = "https://api.github.com"
GITHUB_HOST = "github.com"
NAME_PREFIX_MARKER = "onboarding-"
ENDPOINT_PATH_WORDS = frozenset({"api", "v1", "graphql"})
PERSONAL_DEVELOPMENT_FIELDS: Dict[str, frozenset] = {
    "snowflake": frozenset(
        {
            "role",
            "database",
            "warehouse",
            "schema_name",
            "username",
            "password",
            "private_key",
            "private_key_passphrase",
            "profile",
        }
    ),
    "bigquery": frozenset({"dataset_name", "profile"}),
    "databricks": frozenset({"schema_name", "token", "profile"}),
    "redshift": frozenset({"database", "schema_name", "username", "password", "profile"}),
    "duckdb": frozenset({"schema_name", "profile"}),
    "firebolt": frozenset(
        {
            "account_name",
            "database",
            "engine_name",
            "schema_name",
            "username",
            "password",
            "profile",
        }
    ),
    "extension": frozenset({"schema_name"}),
}
FINISHED_RUN_STATES = {
    BoltRunState.SUCCESS,
    BoltRunState.ERROR,
    BoltRunState.FAILED,
    BoltRunState.CANCELED,
    BoltRunState.SKIPPED,
}


class Status(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    SKIP = "SKIP"


class ConfigError(Exception):
    pass


class StepFailed(Exception):
    pass


class SkipStep(Exception):
    pass


@dataclass
class Outcome:
    detail: str = ""
    notes: List[str] = field(default_factory=list)


@dataclass
class Step:
    number: int
    name: str
    action: Callable[[], Outcome]
    needs: Sequence[int] = ()


@dataclass
class StepResult:
    step: Step
    status: Status
    seconds: float
    detail: str


def describe_error(error: BaseException) -> str:
    if isinstance(error, AdminApiException):
        job = getattr(error, "job", None)
        return f"{error} (job {job.id})" if job is not None else str(error)
    if isinstance(error, (StepFailed, ConfigError)):
        return str(error)
    return f"{type(error).__name__}: {error}"


class StepRunner:
    def __init__(
        self,
        phase: int,
        *,
        out: TextIO = sys.stdout,
        clock: Callable[[], float] = time.monotonic,
        secrets: Iterable[str] = (),
        drain_notes: Callable[[], List[str]] = lambda: [],
    ) -> None:
        self.phase = phase
        self.out = out
        self.clock = clock
        self.secrets = sorted({str(s) for s in secrets if str(s)}, key=len, reverse=True)
        self.drain_notes = drain_notes
        self.results: List[StepResult] = []

    def in_phase(self, step: Step) -> bool:
        if not 1 <= step.number <= TOTAL_STEPS:
            return True
        return self.phase >= 2 or step.number in PHASE_ONE_STEPS

    def status_of(self, number: int) -> Optional[Status]:
        for result in self.results:
            if result.step.number == number:
                return result.status
        return None

    def run(self, step: Step) -> StepResult:
        if not self.in_phase(step):
            return self._record(
                step, Status.SKIP, 0.0, f"phase 2 step, not run with --phase {self.phase}", []
            )
        unmet = [n for n in step.needs if self.status_of(n) != Status.PASS]
        if unmet:
            needed = ", ".join(str(n) for n in unmet)
            return self._record(
                step, Status.SKIP, 0.0, f"needs step {needed}, which did not pass", []
            )
        started = self.clock()
        notes: List[str] = []
        try:
            outcome = step.action()
            status, detail, notes = Status.PASS, outcome.detail, list(outcome.notes)
        except SkipStep as skipped:
            status, detail = Status.SKIP, str(skipped)
        except Exception as error:
            status, detail = Status.FAIL, describe_error(error)
        notes.extend(self.drain_notes())
        return self._record(step, status, self.clock() - started, detail, notes)

    def redact(self, text: str) -> str:
        for secret in self.secrets:
            text = text.replace(secret, "****")
        return text

    def _record(
        self, step: Step, status: Status, seconds: float, detail: str, notes: List[str]
    ) -> StepResult:
        result = StepResult(step=step, status=status, seconds=seconds, detail=self.redact(detail))
        self.results.append(result)
        label = (
            f"{step.number:02d}"
            if 1 <= step.number <= TOTAL_STEPS
            else ("pre" if step.number < 1 else "end")
        )
        line = f"{status.value}  {label:<3}  {step.name:<26} {seconds:7.2f}s  {result.detail}"
        print(line.rstrip(), file=self.out)
        for note in notes:
            print(f"      {self.redact(note)}", file=self.out)
        self.out.flush()
        return result

    @property
    def failed(self) -> bool:
        return any(result.status == Status.FAIL for result in self.results)

    def phase_results(self) -> List[StepResult]:
        return [
            r for r in self.results if 1 <= r.step.number <= TOTAL_STEPS and self.in_phase(r.step)
        ]

    @property
    def criterion_met(self) -> bool:
        return all(r.status == Status.PASS for r in self.phase_results())

    def summary(self) -> str:
        phase_results = self.phase_results()
        counts = {status: sum(1 for r in phase_results if r.status == status) for status in Status}
        not_passed = [f"{r.step.number}" for r in phase_results if r.status != Status.PASS]
        verdict = "met" if not not_passed else f"not met (steps {', '.join(not_passed)})"
        line = (
            f"phase {self.phase}: {counts[Status.PASS]} passed, {counts[Status.FAIL]} failed, "
            f"{counts[Status.SKIP]} skipped of {len(phase_results)} steps; exit criterion {verdict}"
        )
        print(line, file=self.out)
        self.out.flush()
        return line


def _snake(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


@dataclass
class OnboardingConfig:
    name_prefix: str
    git_provider: GitProvider
    git_uri: str
    dbt_project_dir: Optional[str] = None
    dbt_version: Optional[str] = None
    workspace_settings: Dict[str, Any] = field(default_factory=dict)
    development: Optional[Dict[str, Any]] = None
    environments: List[Dict[str, Any]] = field(default_factory=list)
    scheduler_connections: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    variables: Dict[str, Dict[str, str]] = field(default_factory=dict)
    agent: Optional[Dict[str, Any]] = None
    bolt: Dict[str, Any] = field(default_factory=dict)
    notifications: Optional[Dict[str, Any]] = None
    users: List[Dict[str, Any]] = field(default_factory=list)
    job_timeout: float = 600.0
    run_timeout: float = 3600.0
    poll_interval: float = 5.0
    secrets: Set[str] = field(default_factory=set)


WORKSPACE_VARIABLES = ""


class ConfigLoader:
    def __init__(self, path: Path, environ: Mapping[str, str]) -> None:
        self.path = path
        self.base = path.parent
        self.environ = environ
        self.secrets: Set[str] = set()

    def load(self) -> OnboardingConfig:
        try:
            data = YAML(typ="safe").load(self.path.read_text())
        except OSError as error:
            raise ConfigError(f"Can't read the config file {self.path}: {error.strerror}")
        except YAMLError as error:
            raise ConfigError(f"The config file {self.path} is not valid YAML or JSON: {error}")
        if not isinstance(data, dict):
            raise ConfigError("The config file must hold a mapping.")
        data = self.resolve(data)
        workspace = self.mapping(data, "workspace", required=True)
        git = self.mapping(data, "git", required=True)
        prefix = str(workspace.get("name_prefix") or "").strip()
        if not prefix.startswith(NAME_PREFIX_MARKER) or prefix == NAME_PREFIX_MARKER:
            raise ConfigError(
                f"workspace.name_prefix must start with {NAME_PREFIX_MARKER!r} and name the run, "
                f"such as {NAME_PREFIX_MARKER}e2e: the leftover sweep deletes workspaces with it."
            )
        if not git.get("uri"):
            raise ConfigError("git.uri is required.")
        provider = GitProvider(str(git.get("provider", "GITHUB")).upper())
        if provider == GitProvider.UNKNOWN:
            raise ConfigError("git.provider must be GITHUB, GITLAB, BITBUCKET or AZURE_DEVOPS.")
        scheduler = self.mapping(data, "scheduler_connections", required=True)
        if not scheduler:
            raise ConfigError("scheduler_connections needs at least one environment.")
        timeouts = self.mapping(data, "timeouts")
        config = OnboardingConfig(
            name_prefix=prefix,
            git_provider=provider,
            git_uri=str(git["uri"]),
            dbt_project_dir=git.get("dbt_project_dir"),
            dbt_version=workspace.get("dbt_version"),
            workspace_settings=self.mapping(workspace, "settings"),
            development=data.get("development"),
            environments=list(data.get("environments") or []),
            scheduler_connections=scheduler,
            variables=self.variables(self.mapping(data, "environment_variables")),
            agent=data.get("agent"),
            bolt=self.mapping(data, "bolt"),
            notifications=data.get("notifications"),
            users=list(data.get("users") or []),
            job_timeout=float(timeouts.get("job", 600)),
            run_timeout=float(timeouts.get("run", 3600)),
            poll_interval=float(timeouts.get("poll_interval", 5)),
        )
        for slug, connection in scheduler.items():
            self.adapter(connection, f"scheduler_connections.{slug}")
        if config.agent:
            self.adapter(config.agent.get("connection") or {}, "agent.connection")
        if config.development:
            self.adapter(config.development, "development")
        for user in config.users:
            if user.get("account_type", "DEVELOPER") not in UserAccountType.__members__:
                raise ConfigError(f"users: unknown account_type {user.get('account_type')!r}.")
        config.secrets = self.secrets
        return config

    def mapping(
        self, data: Mapping[str, Any], key: str, *, required: bool = False
    ) -> Dict[str, Any]:
        value = data.get(key)
        if value is None and not required:
            return {}
        if not isinstance(value, dict):
            raise ConfigError(f"{key} must be a mapping.")
        return value

    def resolve(self, value: Any, key: str = "") -> Any:
        if isinstance(value, dict) and set(value) == {"fromEnv"}:
            name = str(value["fromEnv"])
            if not self.environ.get(name):
                raise ConfigError(
                    f"{key or 'a value'} reads the environment variable {name}, which is not set."
                )
            return self.secret(self.environ[name])
        if isinstance(value, dict) and set(value) == {"fromFile"}:
            path = self.base / str(value["fromFile"])
            try:
                return self.secret(path.read_text().rstrip("\n"))
            except OSError as error:
                raise ConfigError(f"{key or 'a value'} reads {path}: {error.strerror}")
        if isinstance(value, dict):
            return {k: self.resolve(v, str(k)) for k, v in value.items()}
        if isinstance(value, list):
            return [self.resolve(item, key) for item in value]
        if value is not None and _snake(key) in SECRET_INPUT_FIELDS:
            return self.secret(value)
        return value

    def secret(self, value: T) -> T:
        self.secrets.add(str(value))
        return value

    def variables(self, block: Mapping[str, Any]) -> Dict[str, Dict[str, str]]:
        files: Dict[str, Any] = {}
        if block.get("workspace"):
            files[WORKSPACE_VARIABLES] = block["workspace"]
        for slug, path in (block.get("environments") or {}).items():
            files[str(slug)] = path
        variables: Dict[str, Dict[str, str]] = {}
        for scope, path in files.items():
            if not (self.base / str(path)).is_file():
                raise ConfigError(f"environment_variables: {path} is not a file.")
            values = dotenv_values(self.base / str(path), interpolate=False)
            missing = [name for name, value in values.items() if value is None]
            if missing:
                raise ConfigError(f"{path}: no value for {', '.join(missing)}; write NAME=VALUE.")
            variables[scope] = {name: self.secret(str(value)) for name, value in values.items()}
        return variables

    def adapter(self, connection: Mapping[str, Any], where: str) -> WarehouseAdapterInput:
        try:
            return WarehouseAdapterInput.parse_obj(connection.get("adapter") or {})
        except ValidationError as error:
            fields = ", ".join(".".join(str(p) for p in e["loc"]) for e in error.errors())
            raise ConfigError(f"{where}.adapter has settings the API does not take: {fields}")


class GitHubDeployKeys:
    def __init__(
        self,
        token: str,
        api_url: str = GITHUB_API_URL,
        timeout: float = 30,
        *,
        host: Optional[str] = GITHUB_HOST,
    ) -> None:
        self.api_url = api_url.rstrip("/")
        self.host = host
        self.timeout = timeout
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
        }

    @staticmethod
    def repository_host(uri: str) -> str:
        if "://" in uri:
            return (urlparse(uri).hostname or "").lower()
        return uri.split("@", 1)[-1].split(":", 1)[0].lower()

    def serves(self, uri: str) -> bool:
        return self.host is None or self.repository_host(uri) == self.host

    @staticmethod
    def repository(uri: str) -> str:
        match = re.search(r"[:/]([^/:]+/[^/]+?)(?:\.git)?/?$", uri)
        if match is None:
            raise StepFailed(f"Can't read owner/repository from {uri}.")
        return match.group(1)

    def add(self, repository: str, title: str, public_key: str) -> int:
        response = requests.post(
            f"{self.api_url}/repos/{repository}/keys",
            json={"title": title, "key": public_key, "read_only": False},
            headers=self.headers,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return int(response.json()["id"])

    def keys(self, repository: str) -> List[Dict[str, Any]]:
        keys: List[Dict[str, Any]] = []
        url: Optional[str] = f"{self.api_url}/repos/{repository}/keys?per_page=100"
        while url:
            response = requests.get(url, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            keys.extend(response.json())
            url = response.links.get("next", {}).get("url")
        return keys

    def delete_titled(self, repository: str, matches: Callable[[str], bool]) -> List[str]:
        deleted = []
        for key in self.keys(repository):
            if matches(str(key.get("title", ""))):
                self.delete(repository, int(key["id"]))
                deleted.append(str(key["title"]))
        return deleted

    def delete(self, repository: str, key_id: int) -> None:
        response = requests.delete(
            f"{self.api_url}/repos/{repository}/keys/{key_id}",
            headers=self.headers,
            timeout=self.timeout,
        )
        if response.status_code != 404:
            response.raise_for_status()


ClientFactory = Callable[..., Paradime]


class Onboarding:
    def __init__(
        self,
        config: OnboardingConfig,
        *,
        api_endpoint: str,
        api_secret: str,
        bootstrap_workspace_uid: str,
        github: Optional[GitHubDeployKeys] = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        client_factory: ClientFactory = Paradime,
        interactive: bool = False,
        now: Optional[datetime] = None,
    ) -> None:
        self.config = config
        self.api_endpoint = api_endpoint
        self.api_secret = api_secret
        self.bootstrap_workspace_uid = bootstrap_workspace_uid
        self.github = github
        self.sleep = sleep
        self.clock = clock
        self.client_factory = client_factory
        self.interactive = interactive
        stamp = (now or datetime.now(timezone.utc)).strftime("%Y%m%d-%H%M%S")
        self.workspace_name = f"{config.name_prefix}-{stamp}"
        self.workspace_uid: Optional[str] = None
        self.github_key_id: Optional[int] = None
        self.deployed_schedules: List[str] = []
        self.notes: List[str] = []
        self.account = self.client(bootstrap_workspace_uid)
        self._target: Optional[Paradime] = None

    def client(self, workspace_uid: str) -> Paradime:
        client = self.client_factory(
            api_endpoint=self.api_endpoint, api_secret=self.api_secret, workspace_uid=workspace_uid
        )
        client.admin.transport.sleep = self._rate_limit_wait
        client.admin.jobs.sleep = self.sleep
        return client

    @property
    def target(self) -> Paradime:
        if self._target is None:
            raise StepFailed("No workspace: step 1 did not create one.")
        return self._target

    def drain_notes(self) -> List[str]:
        notes, self.notes = self.notes, []
        return notes

    def _rate_limit_wait(self, seconds: float) -> None:
        self.notes.append(f"RATE_LIMITED: retried after {seconds:g}s")
        self.sleep(seconds)

    def _wait_for_job(self, client: Paradime, job_id: str) -> Any:
        return client.admin.jobs.wait(
            job_id, timeout=self.config.job_timeout, poll_interval=self.config.poll_interval
        )

    def steps(self) -> List[Step]:
        return [
            Step(1, "createWorkspace", self.create_workspace),
            Step(2, "gitDeployKey", self.deploy_key, needs=(1,)),
            Step(3, "connectGitRepository", self.connect_git, needs=(2,)),
            Step(4, "updateWorkspace", self.update_workspace, needs=(1,)),
            Step(5, "devConnection", self.development_connection, needs=(1,)),
            Step(6, "createEnvironment", self.create_environments, needs=(1,)),
            Step(7, "schedulerConnections", self.scheduler_connections, needs=(6,)),
            Step(8, "setEnvironmentVariables", self.environment_variables, needs=(6,)),
            Step(9, "agentEnvironment", self.agent_environment, needs=(1,)),
            Step(10, "deployBoltSchedules", self.deploy_schedules, needs=(3,)),
            Step(11, "notifications", self.notifications, needs=(1,)),
            Step(12, "inviteUser", self.invite_users, needs=(1,)),
            Step(13, "triggerBoltRun", self.bolt_run, needs=(3, 7)),
            Step(14, "workspaceSetupStatus", self.setup_status, needs=(1,)),
        ]

    def leftovers_step(self) -> Step:
        return Step(0, "remove leftovers", self.remove_leftovers)

    def cleanup_step(self, *, keep: bool = False) -> Step:
        return Step(TOTAL_STEPS + 1, "deleteWorkspace (cleanup)", lambda: self.clean_up(keep=keep))

    def _github_repository(self) -> Optional[str]:
        if self._key_handling_skipped() is not None or self.github is None:
            return None
        return self.github.repository(self.config.git_uri)

    def _key_handling_skipped(self) -> Optional[str]:
        if self.github is None:
            return "no PARADIME_ONBOARDING_GITHUB_TOKEN"
        if self.config.git_provider != GitProvider.GITHUB:
            return f"the provider is {self.config.git_provider.value}, not GITHUB"
        if not self.github.serves(self.config.git_uri):
            host = self.github.repository_host(self.config.git_uri)
            return (
                f"{host} is not {self.github.host}; set PARADIME_ONBOARDING_GITHUB_API_URL "
                "to its GitHub API to add the key there"
            )
        return None

    def is_run_name(self, name: str) -> bool:
        pattern = re.escape(self.config.name_prefix) + r"-\d{8}-\d{6}"
        return re.fullmatch(pattern, name) is not None

    def remove_leftovers(self) -> Outcome:
        leftovers = [
            workspace
            for workspace in self.account.admin.workspaces.list()
            if self.is_run_name(workspace.name) and workspace.uid != self.bootstrap_workspace_uid
        ]
        for workspace in leftovers:
            self.account.admin.workspaces.delete(workspace.uid)
        parts = [f"deleted {len(leftovers)} leftover workspace(s)"]
        parts.extend(f"{w.name} ({w.uid})" for w in leftovers)
        repository = self._github_repository()
        if repository is not None and self.github is not None:
            keys = self.github.delete_titled(repository, self.is_run_name)
            parts.append(f"deleted {len(keys)} leftover GitHub deploy key(s)")
        return Outcome(", ".join(parts) if leftovers or repository else "none found")

    def create_workspace(self) -> Outcome:
        workspace = self.account.admin.workspaces.create(
            self.workspace_name, dbt_version=self.config.dbt_version
        )
        self.workspace_uid = workspace.uid
        self._target = self.client(workspace.uid)
        return Outcome(f"{workspace.name} ({workspace.uid}), dbt {workspace.dbt_version}")

    def deploy_key(self) -> Outcome:
        repository = self.target.admin.git.get_repository()
        if repository is None or repository.deploy_key is None:
            raise StepFailed("The workspace returned no deploy key.")
        public_key = repository.deploy_key.public_key
        key_type, _, key_body = public_key.strip().partition(" ")
        shown = f"{key_type} ...{key_body.split(' ')[0][-12:]}"
        github_repository = self._github_repository()
        if github_repository is not None and self.github is not None:
            self.github_key_id = self.github.add(github_repository, self.workspace_name, public_key)
            return Outcome(f"{shown}, added to {github_repository} as a write deploy key")
        notes = [f"add this deploy key with write access to {self.config.git_uri}:", public_key]
        if self.interactive:
            for note in notes:
                print(f"      {note}")
            input("      Press Enter once the key is added. ")
            return Outcome(f"{shown}, added by hand")
        return Outcome(f"{shown}, not added: {self._key_handling_skipped()}", notes)

    def connect_git(self) -> Outcome:
        git = self.target.admin.git
        connection = DeployKeyConnectionInput(
            provider=self.config.git_provider,
            uri=self.config.git_uri,
            **given(dbt_project_dir=self.config.dbt_project_dir or UNSET),
        )
        result = git.connect(connection)
        job_detail = "no job: already connected"
        if result.job is not None:
            job = self._wait_for_job(self.target, result.job.id)
            job_detail = f"job {job.id} {job.status.value}"
        repository: Optional[GitRepository] = git.get_repository()
        state = repository.state if repository is not None else GitRepositoryState.UNKNOWN
        if state != GitRepositoryState.CONNECTED:
            raise StepFailed(f"{job_detail}, but the repository is {state.value}")
        return Outcome(f"{self.config.git_uri} {state.value}, {job_detail}")

    def update_workspace(self) -> Outcome:
        settings = {
            name: self.config.workspace_settings[name]
            for name in (
                "dbt_version",
                "dbt_partial_parsing",
                "default_branch",
                "schedule_deployment_branch",
            )
            if name in self.config.workspace_settings
        }
        if not settings:
            raise SkipStep("no workspace.settings in the config")
        workspace = self.target.admin.workspaces.update(self.target_uid, **settings)
        wrong = [name for name, value in settings.items() if getattr(workspace, name) != value]
        if wrong:
            raise StepFailed(f"the workspace reads back other values for {', '.join(wrong)}")
        return Outcome(", ".join(f"{name}={value}" for name, value in settings.items()))

    @property
    def target_uid(self) -> str:
        if self.workspace_uid is None:
            raise StepFailed("No workspace: step 1 did not create one.")
        return self.workspace_uid

    def development_connection(self) -> Outcome:
        development = self.config.development
        if not development:
            raise SkipStep("no development block in the config")
        adapter, left_out = workspace_level_adapter(
            WarehouseAdapterInput.parse_obj(development.get("adapter") or {})
        )
        saved = self.target.admin.connections.set(
            adapter,
            scope=ConnectionScope.DEVELOPMENT_SHARED,
            connection_name=development.get("connection_name"),
            target_name=development.get("target_name"),
            workspace_uid=self.target_uid,
        )
        detail = f"{saved.type.value} {saved.connection_name or ''!r} saved"
        if left_out:
            detail += f"; left out per-user fields: {', '.join(left_out)}"
        return Outcome(detail)

    def create_environments(self) -> Outcome:
        environments = self.target.admin.environments
        for environment in self.config.environments:
            environments.create(
                EnvironmentType.SCHEDULER,
                str(environment.get("name") or environment["slug"]),
                slug=str(environment["slug"]),
                branch=environment.get("branch"),
                dbt_version=environment.get("dbt_version"),
            )
        slugs = {e.slug for e in environments.list() if e.type == EnvironmentType.SCHEDULER}
        expected = {"production"} | {str(e["slug"]) for e in self.config.environments}
        missing = sorted(expected - slugs)
        if missing:
            raise StepFailed(f"missing after create: {', '.join(missing)}")
        return Outcome(f"Bolt environments: {', '.join(sorted(slugs))}")

    def _set_connection(
        self, scope: ConnectionScope, slug: str, connection: Mapping[str, Any]
    ) -> str:
        saved = self.target.admin.connections.set(
            WarehouseAdapterInput.parse_obj(connection.get("adapter") or {}),
            scope=scope,
            environment_slug=slug,
            connection_name=connection.get("connection_name"),
            target_name=connection.get("target_name"),
            threads=connection.get("threads"),
        )
        return f"{slug}: {saved.type.value}"

    def scheduler_connections(self) -> Outcome:
        saved = [
            self._set_connection(ConnectionScope.SCHEDULER, slug, connection)
            for slug, connection in self.config.scheduler_connections.items()
        ]
        return Outcome(", ".join(saved))

    def environment_variables(self) -> Outcome:
        if not self.config.variables:
            raise SkipStep("no environment_variables in the config")
        parts = []
        for scope, variables in self.config.variables.items():
            result = self.target.admin.environment_variables.set(
                variables, environment_slug=scope or None
            )
            parts.append(
                f"{scope or 'workspace'}: {len(variables)} sent, "
                f"{len(result.changed_names)} changed, {result.total_count} set"
            )
        return Outcome("; ".join(parts))

    def agent_environment(self) -> Outcome:
        agent = self.config.agent
        if not agent:
            raise SkipStep("no agent block in the config (optional step)")
        slug = str(agent.get("slug") or "agent")
        environment = self.target.admin.environments.create(
            EnvironmentType.AGENT, str(agent.get("name") or "Agent"), slug=slug
        )
        saved = self._set_connection(ConnectionScope.AGENT, slug, agent.get("connection") or {})
        defer = environment.defer_environment_slug or environment.defer_schedule_slug or "nothing"
        return Outcome(f"{environment.slug} defers to {defer}, connection {saved}")

    def deploy_schedules(self) -> Outcome:
        job = self.target.admin.bolt.deploy_schedules(branch=self.config.bolt.get("branch"))
        job = self._wait_for_job(self.target, job.id)
        schedules = (job.result or {}).get("schedules") or []
        failed = [s for s in schedules if s.get("error")]
        if failed:
            raise StepFailed(
                "; ".join(f"{s.get('slug')}: {s.get('error')}" for s in failed) + f" (job {job.id})"
            )
        if not schedules:
            raise StepFailed(f"the deploy read no schedules from the repository (job {job.id})")
        self.deployed_schedules = [str(s["slug"]) for s in schedules]
        return Outcome(
            f"job {job.id} {job.status.value}, {len(schedules)} schedule(s): "
            + ", ".join(self.deployed_schedules)
        )

    def notifications(self) -> Outcome:
        settings = self.config.notifications
        if not settings:
            raise SkipStep("no notifications block in the config")
        notifications = self.target.admin.notifications
        parts, notes = [], []
        webhook = settings.get("ms_teams_webhook") or {}
        webhook_names: Optional[List[str]] = None
        if webhook:
            saved = notifications.set_ms_teams_webhook(str(webhook["name"]), str(webhook["url"]))
            webhook_names = [saved.name]
            parts.append(f"Teams webhook {saved.name}")
        email = settings.get("email_enabled")
        alerts = notifications.set_alert_notifications(
            ms_teams_webhook_names=webhook_names,
            email_enabled=None if email is None else bool(email),
        )
        teams = alerts.ms_teams.webhook_names if alerts.ms_teams else []
        enabled = alerts.email.enabled if alerts.email else None
        parts.append(
            f"{alerts.category.value} alerts: Teams {', '.join(teams) or 'none'}, email {enabled}"
        )
        slack = notifications.get_slack()
        if slack is not None:
            parts.append(f"Slack {slack.status.value}")
            if slack.status != SlackInstallationStatus.CONNECTED and slack.install_url:
                notes.append(
                    f"PENDING_USER_ACTION Slack: an account admin connects it at {slack.install_url}"
                )
        return Outcome(", ".join(parts), notes)

    def invite_users(self) -> Outcome:
        if not self.config.users:
            raise SkipStep("no users in the config")
        for user in self.config.users:
            self.target.users.invite(
                str(user["email"]), UserAccountType[str(user.get("account_type", "DEVELOPER"))]
            )
        return Outcome(
            f"invited {len(self.config.users)} user(s) with the legacy paradime.users.invite"
        )

    def _schedule_to_run(self) -> str:
        wanted = self.config.bolt.get("run_schedule")
        if self.deployed_schedules:
            if wanted and wanted not in self.deployed_schedules:
                raise StepFailed(f"bolt.run_schedule {wanted!r} was not deployed in step 10")
            return str(wanted or self.deployed_schedules[0])
        if wanted:
            return str(wanted)
        create = self.config.bolt.get("create_schedule")
        if not create:
            raise StepFailed(
                "no schedule to run: set bolt.run_schedule or bolt.create_schedule, "
                "or run --phase 2 to deploy the repository's schedules"
            )
        return self.target.bolt.create_schedule(
            name=str(create.get("name") or "onboarding acceptance run"),
            schedule=str(create.get("schedule") or "OFF"),
            environment=str(create.get("environment") or "production"),
            commands=list(create.get("commands") or ["dbt debug"]),
        )

    def bolt_run(self) -> Outcome:
        slug = self._schedule_to_run()
        run_id = self.target.bolt.trigger_run(slug=slug, reason=RUN_REASON)
        deadline = self.clock() + self.config.run_timeout
        state = self.target.bolt.get_run_status(run_id)
        while state not in FINISHED_RUN_STATES:
            if self.clock() >= deadline:
                shown = state.value if state else "unknown"
                raise StepFailed(
                    f"run {run_id} of {slug} still {shown} after {self.config.run_timeout:g}s"
                )
            self.sleep(self.config.poll_interval)
            state = self.target.bolt.get_run_status(run_id)
        if state != BoltRunState.SUCCESS:
            raise StepFailed(f"run {run_id} of {slug} ended {state.value if state else 'unknown'}")
        return Outcome(f"run {run_id} of {slug} {state.value} (legacy paradime.bolt.trigger_run)")

    def setup_status(self) -> Outcome:
        status = self.target.admin.setup_status.get(workspace_uid=self.target_uid)
        if status is None:
            raise StepFailed("workspaceSetupStatus returned nothing")
        notes = []
        counts: Dict[str, int] = {}
        missing_required = []
        for row in status.steps or []:
            state = row.status.value
            counts[state] = counts.get(state, 0) + 1
            name = row.step.value + (f" {row.environment_slug}" if row.environment_slug else "")
            note = f"{state:<19} {name}" + ("" if row.required else " (optional)")
            if row.install_url:
                note += f" {row.install_url}"
            if row.status == SetupStepStatus.MISSING and row.detail:
                note += f": {row.detail}"
            notes.append(note)
            if row.status == SetupStepStatus.MISSING and row.required:
                missing_required.append(name)
        products = status.products
        notes.append(
            "products: "
            + ", ".join(
                f"{label} {readiness.status.value}"
                + (
                    f" (missing: {', '.join(s.value for s in readiness.missing)})"
                    if readiness.missing
                    else ""
                )
                + (
                    f" (suggested: {', '.join(s.value for s in readiness.missing_suggested)})"
                    if readiness.missing_suggested
                    else ""
                )
                for label, readiness in (
                    ("Code IDE", products.code_ide),
                    ("Bolt", products.bolt),
                    ("agent", products.agent),
                )
            )
        )
        summary = ", ".join(f"{count} {state}" for state, count in sorted(counts.items()))
        detail = f"isComplete={str(status.is_complete).lower()}, {summary}"
        if missing_required:
            self.notes.extend(notes)
            raise StepFailed(f"{detail}; required steps missing: {', '.join(missing_required)}")
        return Outcome(detail, notes)

    def clean_up(self, *, keep: bool = False) -> Outcome:
        uid = self.workspace_uid
        if uid is None:
            uid = next(
                (
                    w.uid
                    for w in self.account.admin.workspaces.list()
                    if w.name == self.workspace_name
                ),
                None,
            )
        if keep and uid is not None:
            raise SkipStep(f"kept {self.workspace_name} ({uid}) because of --keep")
        parts: List[str] = []
        try:
            if uid is not None:
                self.account.admin.workspaces.delete(uid)
                parts.append(f"deleted {self.workspace_name} ({uid})")
        finally:
            parts.extend(self._remove_deploy_key())
        if not parts:
            raise SkipStep("no workspace was created")
        return Outcome(", ".join(parts))

    def _remove_deploy_key(self) -> List[str]:
        repository = self._github_repository()
        if repository is None or self.github is None or self.github_key_id is None:
            return []
        self.github.delete(repository, self.github_key_id)
        self.github_key_id = None
        return [f"removed the deploy key from {repository}"]


def run_onboarding(onboarding: Onboarding, runner: StepRunner, *, keep: bool = False) -> int:
    try:
        runner.run(onboarding.leftovers_step())
        for step in onboarding.steps():
            runner.run(step)
    finally:
        runner.run(onboarding.cleanup_step(keep=keep))
        runner.summary()
    if runner.failed:
        return 1
    return 0 if runner.criterion_met else EXIT_CRITERION_NOT_MET


def workspace_level_adapter(
    adapter: WarehouseAdapterInput,
) -> Tuple[WarehouseAdapterInput, List[str]]:
    kept: Dict[str, Any] = {}
    left_out: List[str] = []
    for key in type(adapter).__fields__:
        model = getattr(adapter, key)
        if model is None:
            continue
        personal = PERSONAL_DEVELOPMENT_FIELDS.get(key, frozenset())
        fields = type(model).__fields__
        values = model.dict(exclude_unset=True)
        left_out.extend(fields[name].alias for name in values if name in personal)
        kept[key] = type(model)(**{n: v for n, v in values.items() if n not in personal})
    return WarehouseAdapterInput(**kept), sorted(left_out)


def endpoint_secrets(endpoint: str) -> Set[str]:
    segments = urlparse(endpoint).path.split("/")
    return {endpoint} | {s for s in segments if s and s.lower() not in ENDPOINT_PATH_WORDS}


def required_environment(environ: Mapping[str, str], name: str) -> str:
    value = environ.get(name, "").strip()
    if not value:
        raise ConfigError(f"Set the environment variable {name}.")
    return value


def parse_arguments(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Onboard a new workspace with the Paradime Administration API, then delete it. "
            "Credentials come from PARADIME_API_ENDPOINT, PARADIME_API_SECRET (an account key, "
            "prdm_cmp_...) and PARADIME_WORKSPACE_UID (an existing workspace the key reaches, "
            "never deleted); everything else from the config file."
        ),
        epilog=(
            "Exit codes: 0 every step of the phase passed; "
            "1 a step or the cleanup failed, or the run was interrupted; "
            "2 a missing variable or a bad config file, before any request; "
            f"{EXIT_CRITERION_NOT_MET} nothing failed but a step of the phase was skipped."
        ),
    )
    parser.add_argument(
        "--phase",
        type=int,
        choices=(1, 2),
        default=2,
        help="1 runs steps 1-8 and 13; 2 runs all 14 steps (default).",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="The config file (YAML or JSON); defaults to PARADIME_ONBOARDING_CONFIG.",
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        help="Keep the new workspace instead of deleting it; the next run deletes it first.",
    )
    return parser.parse_args(argv)


def main(
    argv: Optional[Sequence[str]] = None,
    environ: Optional[Mapping[str, str]] = None,
    out: TextIO = sys.stdout,
) -> int:
    arguments = parse_arguments(argv)
    logging.getLogger("paradime.apis.admin").setLevel(logging.WARNING)
    environ = os.environ if environ is None else environ
    try:
        api_secret = required_environment(environ, "PARADIME_API_SECRET")
        if not api_secret.startswith(COMPANY_API_TOKEN_PREFIX):
            raise ConfigError(
                f"PARADIME_API_SECRET must be an account API key ({COMPANY_API_TOKEN_PREFIX}...)."
            )
        api_endpoint = required_environment(environ, "PARADIME_API_ENDPOINT")
        bootstrap_uid = required_environment(environ, "PARADIME_WORKSPACE_UID")
        config_path = arguments.config or Path(
            required_environment(environ, "PARADIME_ONBOARDING_CONFIG")
        )
        config = ConfigLoader(config_path, environ).load()
    except ConfigError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    github_token = environ.get("PARADIME_ONBOARDING_GITHUB_TOKEN", "")
    github_api_url = environ.get("PARADIME_ONBOARDING_GITHUB_API_URL", "")
    github: Optional[GitHubDeployKeys] = None
    if github_token and github_api_url:
        github = GitHubDeployKeys(github_token, github_api_url, host=None)
    elif github_token:
        github = GitHubDeployKeys(github_token)
    onboarding = Onboarding(
        config,
        api_endpoint=api_endpoint,
        api_secret=api_secret,
        bootstrap_workspace_uid=bootstrap_uid,
        github=github,
        interactive=sys.stdin.isatty(),
    )
    runner = StepRunner(
        arguments.phase,
        out=out,
        secrets=config.secrets | endpoint_secrets(api_endpoint) | {api_secret, github_token},
        drain_notes=onboarding.drain_notes,
    )
    print(f"onboarding {onboarding.workspace_name}, phase {arguments.phase}", file=out)
    try:
        return run_onboarding(onboarding, runner, keep=arguments.keep)
    except KeyboardInterrupt:
        print("interrupted", file=out)
        return 1


if __name__ == "__main__":
    sys.exit(main())

import re
import secrets
import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Optional, Set, Tuple, Union

import yaml  # type: ignore[import-untyped]
from croniter import croniter  # type: ignore[import-untyped]

from paradime.core.bolt.timezones import SUPPORTED_TIMEZONES
from paradime.tools.pydantic import BaseModel, Extra, root_validator, validator

SCHEDULE_FILE_NAME = "paradime_schedules.yml"
SCHEDULE_FILE_NAMES = ("paradime_schedules.yml", "paradime_schedules.yaml")
SCHEDULES_DIR_NAME = ".bolt"
VALID_ON_EVENTS = ("failed", "passed", "sla")

# Schedule slug format. Mirrors paradb's schedule slug shape:
# <first-24-chars-of-name-normalised>-<6-random-hex-chars>.
# Keep in sync with the paradime-backend slug validator.
SLUG_REGEX = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
SLUG_MAX_LENGTH = 80

DISPLAY_NAME_MAX_LENGTH = 128


def is_valid_slug(name: str) -> bool:
    return bool(name) and len(name) <= SLUG_MAX_LENGTH and SLUG_REGEX.fullmatch(name) is not None


def mint_schedule_slug(display_name: str) -> str:
    """Mint ``<first-24-chars-normalised>-<6-hex>``.

    Same format as the backend's ``mint_schedule_slug`` in paradb. Used locally
    for suggestion text only; the ``paradime bolt mint`` command calls the backend
    to mint the canonical slug.
    """
    normalised = display_name.lower()
    normalised = re.sub(r"[^a-z0-9]", "-", normalised)
    normalised = re.sub(r"-+", "-", normalised)
    normalised = normalised.strip("-")
    normalised = normalised[:24]
    hex_suffix = secrets.token_hex(3)
    return f"{normalised}-{hex_suffix}"


class ParadimeScheduleBase(BaseModel):
    class Config:
        allow_mutation = False
        validate_all = True
        validate_assignment = True
        extra = Extra.forbid


class DeferredSchedule(ParadimeScheduleBase):
    enabled: bool
    deferred_schedule_name: Optional[str]
    deferred_manifest_schedule: Optional[str]
    deferred_schedule_slug: Optional[str] = None
    # `successful_runs_only` is the canonical name used by the JSON schema and the
    # UI. `successful_run_only` is kept for backwards compatibility with existing
    # YAML files.
    successful_runs_only: Optional[bool] = None
    successful_run_only: Optional[bool] = None

    @root_validator()
    @classmethod
    def validate_all_fields_at_the_same_time(cls, values: Any) -> Any:
        # backwards compatability
        deferred_schedule_name = (
            values.get("deferred_schedule_slug")
            or values.get("deferred_schedule_name")
            or values.get("deferred_manifest_schedule")
        )
        if not deferred_schedule_name:
            raise ValueError("Missing deferred_schedule_name")
        values["deferred_schedule_name"] = deferred_schedule_name

        # accept either spelling; prefer the plural canonical form. Default True.
        plural = values.get("successful_runs_only")
        singular = values.get("successful_run_only")
        resolved = plural if plural is not None else singular
        if resolved is None:
            resolved = True
        values["successful_runs_only"] = bool(resolved)
        values["successful_run_only"] = bool(resolved)
        return values


class Hightouch(ParadimeScheduleBase):
    enabled: bool
    sync_on: List[str]
    slugs: List[str]


class ScheduleTrigger(ParadimeScheduleBase):
    enabled: bool
    schedule_name: Optional[str] = None
    schedule_slug: Optional[str] = None
    workspace_name: str
    trigger_on: List[str]

    @root_validator()
    @classmethod
    def resolve_slug(cls, values: Any) -> Any:
        schedule_name = values.get("schedule_slug") or values.get("schedule_name")
        if not schedule_name:
            raise ValueError("Missing schedule_name or schedule_slug")
        values["schedule_name"] = schedule_name
        return values

    @validator("trigger_on")
    def validate_trigger_on(cls, trigger_on: List[str]) -> List[str]:
        for trigger_on_value in trigger_on:
            if trigger_on_value not in VALID_ON_EVENTS:
                raise ValueError(f"'{trigger_on_value}' not a valid event ({VALID_ON_EVENTS})")
        return trigger_on


class NotificationItem(BaseModel):
    # channel and address can be used interchangeably but one is required
    channel: Optional[str]
    address: Optional[str]

    events: List[str]

    @validator("events")
    def validate_events(cls, events: List[str]) -> List[str]:
        for event in events:
            if event.lower() not in VALID_ON_EVENTS:
                raise ValueError(f"'{event}' not a valid event ({VALID_ON_EVENTS})")
        return [event.lower() for event in events]

    @root_validator()
    @classmethod
    def validate_all_fields_at_the_same_time(cls, values: Any) -> Any:
        # channel and address can be used interchangeably
        # with channel being the source of truth.
        channel = values.get("channel") or values.get("address")
        if not channel:
            raise ValueError("Missing 'channel' or 'address'")
        values["channel"] = channel
        return values

    def get_channel(self) -> str:
        assert self.channel
        return self.channel


class Notifications(BaseModel):
    emails: Optional[List[NotificationItem]]
    slack_channels: Optional[List[NotificationItem]]
    microsoft_teams: Optional[List[NotificationItem]]


class IntegrationBase(BaseModel):
    """Base for integration items.

    Validation is intentionally permissive: enum-like fields (state, severity,
    mode, visibility, urgency, ...) are accepted regardless of casing because
    the UI and the backend tolerate both, and the goal of `verify` is to catch
    typos, not to enforce a specific spelling.
    """

    class Config:
        extra = Extra.allow


class IncidentIOIntegration(IntegrationBase):
    status_id: Optional[str] = None
    status: Optional[str] = None
    type_id: Optional[str] = None
    type: Optional[str] = None
    mode: Optional[str] = None
    severity_id: Optional[str] = None
    severity: Optional[str] = None
    visibility: Optional[str] = None


class PagerDutyIntegration(IntegrationBase):
    from_email: Optional[str] = None
    service_id: Optional[str] = None
    service_name: Optional[str] = None
    priority_id: Optional[str] = None
    priority_name: Optional[str] = None
    urgency: Optional[str] = None
    incident_type_name: Optional[str] = None
    incident_type_display_name: Optional[str] = None
    escalation_policy_id: Optional[str] = None
    escalation_policy_name: Optional[str] = None
    assignee_ids: Optional[List[str]] = None
    assignee_names: Optional[List[str]] = None


class DatadogIntegration(IntegrationBase):
    severity: Optional[str] = None
    severity_name: Optional[str] = None
    state: Optional[str] = None
    state_name: Optional[str] = None
    customer_impacted: Optional[bool] = None
    commander_user_id: Optional[str] = None
    commander_user_name: Optional[str] = None
    notification_handles: Optional[List[str]] = None


class NewRelicIntegration(IntegrationBase):
    environment: Optional[str] = None


class Integrations(BaseModel):
    incident_io: Optional[List[IncidentIOIntegration]] = None
    pagerduty: Optional[List[PagerDutyIntegration]] = None
    datadog: Optional[List[DatadogIntegration]] = None
    new_relic: Optional[List[NewRelicIntegration]] = None

    class Config:
        extra = Extra.allow


class ParadimeSchedule(ParadimeScheduleBase):
    name: str
    slug: Optional[str] = None
    display_name: Optional[str] = None
    schedule: str
    timezone: Optional[str] = None
    environment: str
    commands: List[str]

    git_branch: Optional[str] = None
    owner_email: Optional[str] = None

    description: Optional[str] = None

    slack_notify: Union[str, List[str]] = [""]
    slack_on: List[str] = [""]

    email_notify: Union[str, List[str]] = [""]
    email_on: List[str] = [""]

    notifications: Optional[Notifications] = None
    integrations: Optional[Integrations] = None
    sla_minutes: Optional[int] = None

    turbo_ci: Optional[DeferredSchedule] = None
    deferred_schedule: Optional[DeferredSchedule] = None

    hightouch: Optional[Hightouch] = None

    schedule_trigger: Optional[ScheduleTrigger] = None

    trigger_on_merge: Optional[bool] = False

    suspended: Optional[bool] = False

    @validator("display_name")
    def validate_display_name(cls, display_name: Optional[str]) -> Optional[str]:
        if display_name is None:
            return None
        if "\n" in display_name or "\r" in display_name:
            raise ValueError("display_name must not contain newlines")
        if len(display_name) > DISPLAY_NAME_MAX_LENGTH:
            raise ValueError(f"display_name must be {DISPLAY_NAME_MAX_LENGTH} characters or fewer")
        return display_name

    @root_validator()
    @classmethod
    def validate_all_fields(cls, values: Any) -> Any:
        # v3 slug swap: when slug is present, YAML `name` is the human label
        slug = values.get("slug")
        if slug:
            yaml_name = values["name"]
            values["name"] = slug
            values["display_name"] = yaml_name
            if "\n" in yaml_name or "\r" in yaml_name:
                raise ValueError("display_name must not contain newlines")
            if len(yaml_name) > DISPLAY_NAME_MAX_LENGTH:
                raise ValueError(
                    f"display_name must be {DISPLAY_NAME_MAX_LENGTH} characters or fewer"
                )
        return values


class ParadimeSchedules(ParadimeScheduleBase):
    version: int = 1
    schedules: List[ParadimeSchedule]


@dataclass(frozen=True)
class Command:
    as_list: List[str]


def get_slug_format_warnings(schedules: ParadimeSchedules) -> List[str]:
    """Names that aren't slug-format yet — informational only.

    The backend grandfathers existing names so deploys keep working, but new
    schedule names should match the slug format. Surface this as a nudge from
    `verify` without failing it.
    """
    warnings: List[str] = []
    for schedule in schedules.schedules:
        if not is_valid_slug(schedule.name):
            suggested = mint_schedule_slug(schedule.display_name or schedule.name)
            warnings.append(
                f"{schedule.name!r} is not in slug format. "
                f"Run `paradime bolt mint` to auto-generate slugs, "
                f"or use a slug like {suggested!r}."
            )
    return warnings


def is_valid_schedule_at_path(
    file_path: Path,
    existing_names: Optional[Set[str]] = None,
    schedule_trigger_refs: Optional[Set[Tuple[str, str]]] = None,
) -> Optional[str]:
    """Validate a schedule YAML file.

    Args:
        file_path: Path to the schedule YAML file.
        existing_names: Optional set of schedule names already deployed in the
            current workspace. When provided, ``turbo_ci`` / ``deferred_schedule``
            references are allowed to resolve to these names in addition to the
            local YAML names (grandfathering of already-deployed schedules).
        schedule_trigger_refs: Optional set of ``(workspace_name, schedule_name)``
            pairs for schedules deployed across all workspaces. When provided,
            ``schedule_trigger`` references are validated against it (a
            ``schedule_trigger`` may point at a schedule in another workspace).
            When ``None`` (e.g. offline / API unavailable) the cross-workspace
            check is skipped.
    """
    try:
        schedules = _get_schedules(file_path)
    except Exception as e:
        return f"Unable to parse file:\n{e}"

    if not schedules:
        return f"No schedules found in {file_path}"

    # check no duplicate schedule names
    schedule_names = [schedule.name for schedule in schedules.schedules]
    if len(schedule_names) > len(set(schedule_names)):
        return "Schedule names are not unique."

    # All known names: local YAML + backend (if available)
    known_names = set(schedule_names)
    if existing_names:
        known_names |= existing_names

    # check turbo ci / deferred references resolve to known schedules
    # (multiple turbo CI configs are supported)
    for schedule in schedules.schedules:
        if schedule.turbo_ci and schedule.turbo_ci.enabled:
            if schedule.turbo_ci.deferred_schedule_name not in known_names:
                return f"Turbo CI schedule error: '{schedule.turbo_ci.deferred_schedule_name}' does not refer to a known schedule name"

        if schedule.deferred_schedule and schedule.deferred_schedule.enabled:
            if schedule.deferred_schedule.deferred_schedule_name not in known_names:
                return f"Deferred schedule error: '{schedule.deferred_schedule.deferred_schedule_name}' does not refer to a known schedule name"

        # schedule_trigger may point at a schedule in any workspace; validate the
        # (workspace_name, schedule_name) pair (a local schedule also counts).
        if (
            schedule.schedule_trigger
            and schedule.schedule_trigger.enabled
            and schedule_trigger_refs
        ):
            trigger = schedule.schedule_trigger
            ref = (trigger.workspace_name, trigger.schedule_name)
            if ref not in schedule_trigger_refs and trigger.schedule_name not in schedule_names:
                return (
                    f"Schedule trigger error: '{trigger.schedule_name}' does not refer to a known "
                    f"schedule in workspace '{trigger.workspace_name}'"
                )

    # Verify schedules individually
    for schedule in schedules.schedules:
        if error := verify_single_schedule(schedule):
            return error

    return None


def verify_single_schedule(schedule: ParadimeSchedule) -> Optional[str]:
    schedule_name = schedule.name

    # check there are commands
    if not schedule.commands:
        return f"{schedule_name}: Missing commands."

    # verify cron schedule
    if (
        schedule.schedule.lower() != "off"
        and not croniter.is_valid(schedule.schedule)
        and not _cron_schedule_is_non_standard(schedule.schedule)
    ):
        return f"{schedule_name}: Schedule '{schedule.schedule}' is not a valid cron schedule."

    # verify timezone
    if schedule.timezone is not None and schedule.timezone not in SUPPORTED_TIMEZONES:
        return f"{schedule_name}: Timezone '{schedule.timezone}' is not a valid timezone."

    # verify 'on' events
    for slack_on in schedule.slack_on:
        if slack_on != "":
            if slack_on not in VALID_ON_EVENTS:
                return (
                    f"{schedule_name}: Slack on '{slack_on}' is not valid - use: {VALID_ON_EVENTS}."
                )

    for email_on in schedule.email_on:
        if email_on != "":
            if email_on not in VALID_ON_EVENTS:
                return (
                    f"{schedule_name}: Email on '{email_on}' is not valid - use: {VALID_ON_EVENTS}."
                )

    return None


def parse_command(command: str) -> Command:
    """Convert a command presented a string to a command presented as list."""

    cmd_as_list = shlex.split(command)
    return Command(as_list=cmd_as_list)


def _find_schedule_files(path: Path) -> List[Path]:
    """Discover schedule YAML files from a path.

    ``path`` can be:
    - A direct file path (returned as-is if it exists).
    - A directory that contains ``paradime_schedules.{yml,yaml}`` or a
      ``.bolt/`` sub-directory with YAML files.
    """
    if path.is_file():
        return [path]

    # Treat path as a directory (project root).
    root = path if path.is_dir() else path.parent
    files: List[Path] = []

    # Collect from .bolt/ directory
    bolt_dir = root / SCHEDULES_DIR_NAME
    if bolt_dir.is_dir():
        files.extend(
            sorted(p for p in bolt_dir.rglob("*") if p.is_file() and p.suffix in (".yaml", ".yml"))
        )

    # Collect flat file(s)
    for name in SCHEDULE_FILE_NAMES:
        flat = root / name
        if flat.is_file():
            files.append(flat)

    return files


def _get_schedules(path: Path) -> Optional[ParadimeSchedules]:
    """Parse schedule YAML file(s).

    Supports a single flat file (``paradime_schedules.{yml,yaml}``) as well as
    a ``.bolt/`` directory containing multiple YAML files.  When multiple files
    are found their schedule lists are merged into one ``ParadimeSchedules``.
    """
    files = _find_schedule_files(path)
    if not files:
        return None

    all_schedules: List[ParadimeSchedule] = []
    for f in files:
        parsed_yaml = yaml.safe_load(f.read_text())
        if not parsed_yaml:
            continue
        parsed = ParadimeSchedules.parse_obj(parsed_yaml)
        all_schedules.extend(parsed.schedules)

    if not all_schedules:
        return None

    return ParadimeSchedules(schedules=all_schedules)


def _cron_schedule_is_non_standard(schedule: str) -> bool:
    # paradime only supports standard cron schedules
    # see https://crontab.guru/
    return schedule.endswith("7")

import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Optional, Union

import yaml  # type: ignore[import-untyped]
from croniter import croniter  # type: ignore[import-untyped]

from paradime.tools.pydantic import BaseModel, Extra, root_validator

SCHEDULE_FILE_NAME = "paradime_schedules.yml"
VALID_ON_EVENTS = ("failed", "passed")

ALLOWED_COMMANDS = ["dbt", "re_data", "edr", "lightdash"]


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
    successful_run_only: Optional[bool] = True

    @root_validator()
    @classmethod
    def validate_all_fields_at_the_same_time(cls, values: Any) -> Any:
        # backwards compatability
        deferred_schedule_name = values.get("deferred_schedule_name") or values.get(
            "deferred_manifest_schedule"
        )
        if not deferred_schedule_name:
            raise ValueError("Missing deferred_schedule_name")
        values["deferred_schedule_name"] = deferred_schedule_name

        # successful_run_only should default to true if not set
        values["successful_run_only"] = bool(values.get("successful_run_only", True))
        return values


class Hightouch(ParadimeScheduleBase):
    enabled: bool
    sync_on: List[str]
    slugs: List[str]


class ParadimeSchedule(ParadimeScheduleBase):
    name: str
    schedule: str
    environment: str
    commands: List[str]

    git_branch: Optional[str] = None
    owner_email: Optional[str] = None

    slack_notify: Union[str, List[str]] = [""]
    slack_on: List[str] = [""]

    email_notify: Union[str, List[str]] = [""]
    email_on: List[str] = [""]

    turbo_ci: Optional[DeferredSchedule] = None
    deferred_schedule: Optional[DeferredSchedule] = None

    hightouch: Optional[Hightouch] = None


class ParadimeSchedules(ParadimeScheduleBase):
    version: int = 1
    schedules: List[ParadimeSchedule]


@dataclass(frozen=True)
class Command:
    as_list: List[str]


def is_valid_schedule_at_path(file_path: Path) -> Optional[str]:
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

    # check only one turbo ci config
    found_turbo_ci = False
    for schedule in schedules.schedules:
        if schedule.turbo_ci and schedule.turbo_ci.enabled:
            if found_turbo_ci:
                return "There should only be one Turbo CI config."
            else:
                found_turbo_ci = True

            if schedule.turbo_ci.deferred_manifest_schedule not in schedule_names:
                return f"Deferred_manifest_schedule: '{schedule.turbo_ci.deferred_manifest_schedule}' does not refer to another schedule name"

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

    # check all commands start with 'dbt'
    for command in schedule.commands:
        if not is_allowed_command(parse_command(command)):
            return f"{schedule_name}: Command {command!r} is not an allowed command. Allowed commands are: {ALLOWED_COMMANDS}."

    # verify cron schedule
    if (
        schedule.schedule.lower() != "off"
        and not croniter.is_valid(schedule.schedule)
        and not _cron_schedule_is_non_standard(schedule.schedule)
    ):
        return f"{schedule_name}: Schedule '{schedule.schedule}' is not a valid cron schedule."

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


def is_allowed_command(command: Command) -> bool:
    """Check if the command is an allowed command."""

    cmd = command.as_list
    return bool(cmd) and cmd[0] in ALLOWED_COMMANDS


def _get_schedules(path: Path) -> Optional[ParadimeSchedules]:
    """Parse the yaml file."""

    # Get the schedules.
    if path.is_file():
        parsed_yaml = yaml.safe_load(path.read_text())
        return ParadimeSchedules.parse_obj(parsed_yaml)
    return None


def _cron_schedule_is_non_standard(schedule: str) -> bool:
    # paradime only supports standard cron schedules
    # see https://crontab.guru/
    return schedule.endswith("7")

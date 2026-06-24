from enum import Enum
from typing import List, Optional

from paradime.tools.pydantic import BaseModel


class BoltRunState(str, Enum):
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"
    FAILED = "FAILED"
    CANCELED = "CANCELED"
    SKIPPED = "SKIPPED"

    @classmethod
    def from_str(cls, value: str) -> Optional["BoltRunState"]:
        try:
            return BoltRunState(value)
        except ValueError:
            return None


class BoltDeferredSchedule(BaseModel):
    enabled: bool
    deferred_schedule_name: Optional[str]
    successful_run_only: bool


class BoltNotificationItem(BaseModel):
    channel: Optional[str]
    events: Optional[List[str]]
    template_slug: Optional[str]
    template_name: Optional[str]


class BoltNotifications(BaseModel):
    email_notifications: Optional[List[BoltNotificationItem]]
    slack_notifications: Optional[List[BoltNotificationItem]]
    ms_teams_notifications: Optional[List[BoltNotificationItem]]


class BoltSchedule(BaseModel):
    name: str
    slug: Optional[str]
    display_name: Optional[str]
    schedule: str
    owner: Optional[str]
    last_run_at: Optional[str]
    last_run_state: Optional[str]
    next_run_at: Optional[str]
    id: int
    uuid: str
    source: str
    deferred_schedule: Optional[BoltDeferredSchedule]
    turbo_ci: Optional[BoltDeferredSchedule]
    commands: Optional[List[str]]
    git_branch: Optional[str]
    slack_on: Optional[List[str]]
    slack_notify: Optional[List[str]]
    email_on: Optional[List[str]]
    email_notify: Optional[List[str]]
    notifications: Optional[BoltNotifications]


class BoltSchedules(BaseModel):
    schedules: List[BoltSchedule]
    total_count: int


class BoltScheduleInfo(BaseModel):
    name: str
    commands: List[str]
    schedule: str
    uuid: str
    source: str
    owner: Optional[str]
    latest_run_id: Optional[int]


class BoltCommand(BaseModel):
    id: int
    command: str
    start_dttm: str
    end_dttm: str
    stdout: str
    stderr: str
    return_code: Optional[int]


class BoltCommandArtifact(BaseModel):
    id: int
    path: str


class BoltRunGitInfo(BaseModel):
    branch: Optional[str]
    commit_hash: Optional[str]
    pull_request_id: Optional[str]


class BoltRun(BaseModel):
    id: int
    state: str
    actor: str
    actor_email: Optional[str]
    parent_schedule_run_id: Optional[int]
    start_dttm: str
    end_dttm: Optional[str]
    git_info: BoltRunGitInfo


class BoltScheduleRuns(BaseModel):
    ok: bool
    runs: List[BoltRun]


class BoltLogStream(str, Enum):
    STDOUT = "STDOUT"
    STDERR = "STDERR"


class BoltLogLine(BaseModel):
    stream: BoltLogStream
    line: str


class BoltCommandLogs(BaseModel):
    lines: List[BoltLogLine]
    cursor: str
    finished: bool


def _snake_to_camel(snake: str) -> str:
    head, *tail = snake.split("_")
    return head + "".join(part.title() for part in tail)


class _BoltInputBase(BaseModel):
    """Base for ``create_schedule`` input models.

    Subclass fields use snake_case in Python and serialize to camelCase to
    match the GraphQL ``BoltScheduleInput`` shape.
    """

    class Config:
        alias_generator = _snake_to_camel
        allow_population_by_field_name = True


class BoltNotificationChannelInput(_BoltInputBase):
    """One notification target (a Slack channel, Teams channel, or email address)."""

    channel: str
    events: List[str]
    template_slug: Optional[str] = None


class BoltNotificationsInput(_BoltInputBase):
    """Notification routing for a Bolt schedule.

    Each list element is a separate channel; pass an empty list / omit to
    disable that transport.
    """

    email_notifications: Optional[List[BoltNotificationChannelInput]] = None
    slack_notifications: Optional[List[BoltNotificationChannelInput]] = None
    teams_notifications: Optional[List[BoltNotificationChannelInput]] = None


class BoltIncidentIoConfigInput(_BoltInputBase):
    """incident.io auto-incident config. ``visibility`` is required."""

    visibility: str
    status_id: Optional[str] = None
    status: Optional[str] = None
    type_id: Optional[str] = None
    type: Optional[str] = None
    mode: Optional[str] = None
    severity_id: Optional[str] = None
    severity: Optional[str] = None


class BoltPagerDutyConfigInput(_BoltInputBase):
    """PagerDuty auto-incident config."""

    from_email: str
    service_id: str
    service_name: str
    incident_type_display_name: str
    incident_type_name: str
    priority_id: Optional[str] = None
    priority_name: Optional[str] = None
    urgency: Optional[str] = None
    escalation_policy_id: Optional[str] = None
    escalation_policy_name: Optional[str] = None
    assignee_ids: Optional[List[str]] = None
    assignee_names: Optional[List[str]] = None


class BoltDatadogConfigInput(_BoltInputBase):
    """Datadog incident config."""

    severity: str
    severity_name: str
    customer_impacted: bool
    state: Optional[str] = None
    state_name: Optional[str] = None
    commander_user_id: Optional[str] = None
    commander_user_name: Optional[str] = None
    notification_handles: Optional[List[str]] = None


class BoltNewRelicConfigInput(_BoltInputBase):
    """New Relic incident config."""

    environment: str


class BoltIntegrationsInput(_BoltInputBase):
    """Third-party incident integrations fired on run failures.

    Each list element is one configured integration of that type.
    """

    incident_io: Optional[List[BoltIncidentIoConfigInput]] = None
    pagerduty: Optional[List[BoltPagerDutyConfigInput]] = None
    datadog: Optional[List[BoltDatadogConfigInput]] = None
    new_relic: Optional[List[BoltNewRelicConfigInput]] = None


class BoltSelfHealingConfigInput(_BoltInputBase):
    """Paradime self-healing agent config (auto-retry + Slack notice)."""

    enabled: bool
    slack_channel: Optional[str] = None
    agent_name: Optional[str] = None


class BoltDeferredScheduleConfigInput(_BoltInputBase):
    """Shape shared by ``deferred_schedule`` (Slim CI) and ``turbo_ci``."""

    enabled: bool
    successful_run_only: bool
    deferred_schedule_name: Optional[str] = None


class BoltScheduleTriggerInput(_BoltInputBase):
    """Parent-schedule trigger — run this schedule when another finishes."""

    enabled: bool
    schedule_name: Optional[str] = None
    workspace_name: Optional[str] = None
    trigger_on: Optional[List[str]] = None


class BoltEnvironmentVariableInput(_BoltInputBase):
    """A single env-var override passed to the schedule's runtime."""

    key: str
    value: str

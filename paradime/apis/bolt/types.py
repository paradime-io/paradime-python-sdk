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


class BoltSchedule(BaseModel):
    name: str
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

from enum import Enum
from typing import List, Optional

from paradime.tools.pydantic import BaseModel


class DinoaiAgentRunStatus(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    EXPIRED = "EXPIRED"  # terminal: the agent pod never started
    STOPPED = "STOPPED"  # terminal: the run was stopped before it finished
    # Fallback for statuses this SDK version doesn't know about yet — treated as
    # non-terminal so an older SDK keeps polling instead of crashing.
    UNKNOWN = "UNKNOWN"

    @classmethod
    def _missing_(cls, value: object) -> "DinoaiAgentRunStatus":
        return cls.UNKNOWN

    @classmethod
    def from_str(cls, value: str) -> Optional["DinoaiAgentRunStatus"]:
        status = cls(value)
        return None if status is cls.UNKNOWN and value != cls.UNKNOWN.value else status


class DinoaiAgentMessage(BaseModel):
    ts: str
    role: str
    content: str


class DinoaiAgentTriggerResult(BaseModel):
    ok: bool
    agent_session_id: str
    status: str


class DinoaiAgentRun(BaseModel):
    ok: bool
    status: DinoaiAgentRunStatus
    messages: List[DinoaiAgentMessage]
    child_session_ids: List[str]
    workspace_uid: Optional[str]

from enum import Enum
from typing import List, Optional

from paradime.tools.pydantic import BaseModel


class DinoaiAgentRunStatus(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

    @classmethod
    def from_str(cls, value: str) -> Optional["DinoaiAgentRunStatus"]:
        try:
            return DinoaiAgentRunStatus(value)
        except ValueError:
            return None


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

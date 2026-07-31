from enum import Enum
from typing import List, Optional

from paradime.tools.models import ParadimeResponseModel


class DinoaiAgentRunStatus(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    EXPIRED = "EXPIRED"  # terminal: the agent pod never started

    @classmethod
    def from_str(cls, value: str) -> Optional["DinoaiAgentRunStatus"]:
        try:
            return DinoaiAgentRunStatus(value)
        except ValueError:
            return None


class DinoaiAgentMessage(ParadimeResponseModel):
    ts: str
    role: str
    content: str


class DinoaiAgentTriggerResult(ParadimeResponseModel):
    ok: bool
    agent_session_id: str
    status: str


class DinoaiAgentRun(ParadimeResponseModel):
    ok: bool
    status: DinoaiAgentRunStatus
    messages: List[DinoaiAgentMessage]
    child_session_ids: List[str]
    workspace_uid: Optional[str]

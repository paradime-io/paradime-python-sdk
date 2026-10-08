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
    # Set when Paradime adjusted the request, for example an unknown model family.
    # Only requested when a model family is passed.
    warning: Optional[str] = None


class DinoaiAgentStep(BaseModel):
    """One step of a run: a tool call, or text from the user or the agent.

    ``index`` is the step's position in the run. Pass the last one you read as
    ``steps_after`` to fetch only newer steps.
    """

    index: int
    role: str  # USER, AGENT or TOOL
    tool_name: Optional[str] = None
    # Only with include_tool_io: the call's arguments as JSON, and for a TOOL
    # step its output. They can hold raw query results and file contents.
    tool_input: Optional[str] = None
    content: Optional[str] = None
    truncated: bool = False


class DinoaiAgentStartupStep(BaseModel):
    label: str
    done: bool


class DinoaiAgentRun(BaseModel):
    ok: bool
    status: DinoaiAgentRunStatus
    messages: List[DinoaiAgentMessage]
    child_session_ids: List[str]
    workspace_uid: Optional[str]
    # Only with get_run(include_steps=True). None when not requested, or when
    # the backend could not read them.
    steps: Optional[List[DinoaiAgentStep]] = None
    startup_steps: Optional[List[DinoaiAgentStartupStep]] = None

from enum import Enum
from typing import Optional

from paradime.tools.pydantic import BaseModel


class ReportStatus(str, Enum):
    AVAILABLE = "AVAILABLE"
    FAILED = "FAILED"
    IN_PROGRESS = "IN_PROGRESS"


class Report(BaseModel):
    uuid: str
    message: Optional[str] = None
    status: ReportStatus
    url: Optional[str] = None
    result_json: Optional[str] = None
    result_markdown: Optional[str] = None

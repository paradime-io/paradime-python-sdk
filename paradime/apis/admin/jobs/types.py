import json
from datetime import datetime
from typing import Any, Optional

from paradime.apis.admin.exception import ErrorCode, parse_error_code
from paradime.apis.admin.types import AdminModel, OpenEnum
from paradime.tools.pydantic import validator

ADMIN_JOB_FIELDS = """
    id
    kind
    status
    failure {
        code
        reason
    }
    result
    createdAt
    updatedAt
"""


class AdminJobStatus(OpenEnum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


class AdminJobFailure(AdminModel):
    code: str
    reason: str

    @property
    def error_code(self) -> Optional[ErrorCode]:
        return parse_error_code(self.code)


class AdminJob(AdminModel):
    id: str
    kind: str
    status: AdminJobStatus
    failure: Optional[AdminJobFailure] = None
    result: Optional[Any] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @validator("result", pre=True)
    def _parse_result(cls, value: Any) -> Any:
        if isinstance(value, str):
            try:
                return json.loads(value)
            except ValueError:
                return value
        return value

    @property
    def is_finished(self) -> bool:
        return self.status in (AdminJobStatus.SUCCEEDED, AdminJobStatus.FAILED)

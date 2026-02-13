from datetime import datetime
from typing import Optional

from paradime.tools.pydantic import BaseModel


class AuditLog(BaseModel):
    id: int
    created_dttm: datetime
    updated_dttm: datetime
    workspace_id: int
    workspace_name: str
    actor_type: str
    actor_user_id: int
    actor_email: Optional[str]
    event_source_id: int
    event_source: str
    event_id: int
    event_type: str
    metadata_json: Optional[str]

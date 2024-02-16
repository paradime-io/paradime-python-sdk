from datetime import datetime

from pydantic import BaseModel


class AuditLog(BaseModel):
    id: int
    created_dttm: datetime
    updated_dttm: datetime
    workspace_id: int
    workspace_name: str
    actor_type: str
    actor_user_id: int
    actor_email: str | None
    event_source_id: int
    event_source: str
    event_id: int
    event_type: str
    metadata_json: str | None

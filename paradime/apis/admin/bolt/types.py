from datetime import datetime
from typing import List, Optional

from paradime.apis.admin.types import AdminInputModel, AdminModel

BOLT_SCHEDULE_DEFINITION_FIELDS = """
    slug
    name
    schedule
    timezone
    environment
    gitBranch
    ownerEmail
    description
    slaSeconds
    commands
    suspended
    triggerOnMerge
    triggerOnWebhook
    updatedAt
"""

TRIGGER_URL_FIELDS = """
    publicId
    name
    enabled
    url
    scheduleSlug
    scheduleFound
    createdAt
    lastEventAt
    secretIsSet
    secretUpdatedAt
"""


class BoltScheduleDefinition(AdminModel):
    slug: str
    name: str
    schedule: Optional[str] = None
    timezone: Optional[str] = None
    environment: Optional[str] = None
    git_branch: Optional[str] = None
    owner_email: Optional[str] = None
    description: Optional[str] = None
    sla_seconds: Optional[int] = None
    commands: Optional[List[str]] = None
    suspended: Optional[bool] = None
    trigger_on_merge: Optional[bool] = None
    trigger_on_webhook: Optional[bool] = None
    updated_at: Optional[datetime] = None


class TriggerUrl(AdminModel):
    public_id: str
    name: str
    enabled: Optional[bool] = None
    url: Optional[str] = None
    schedule_slug: Optional[str] = None
    schedule_found: Optional[bool] = None
    created_at: Optional[datetime] = None
    last_event_at: Optional[datetime] = None
    secret_is_set: Optional[bool] = None
    secret_updated_at: Optional[datetime] = None


class TriggerUrlWithSecret(AdminModel):
    trigger_url: TriggerUrl
    secret: str


class CreateTriggerUrlInput(AdminInputModel):
    schedule_slug: str
    name: str


class TriggerUrlKeyInput(AdminInputModel):
    public_id: str


class SetTriggerUrlEnabledInput(AdminInputModel):
    public_id: str
    enabled: bool


class DeployBoltSchedulesInput(AdminInputModel):
    branch: Optional[str] = None

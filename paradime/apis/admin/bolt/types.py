from datetime import datetime
from typing import List, Optional

from paradime.apis.admin.types import (
    BOLT_SCHEDULE_LINK_FIELDS,
    ENVIRONMENT_LINK_FIELDS,
    AdminInputModel,
    AdminModel,
    BoltScheduleLink,
    EnvironmentLink,
)

BOLT_SCHEDULE_DEFINITION_FIELDS = f"""
    slug
    name
    schedule
    timezone
    environment {{
        {ENVIRONMENT_LINK_FIELDS}
    }}
    gitBranch
    ownerEmail
    description
    slaSeconds
    commands
    isSuspended
    triggerOnMerge
    triggerOnWebhook
    updatedAt
"""

BOLT_SCHEDULE_DEFINITION_LINKS = frozenset({"environment"})

TRIGGER_URL_FIELDS = f"""
    publicId
    name
    isEnabled
    url
    schedule {{
        {BOLT_SCHEDULE_LINK_FIELDS}
    }}
    createdAt
    lastEventAt
    hasSecret
    secretUpdatedAt
"""

TRIGGER_URL_LINKS = frozenset({"schedule"})


class BoltScheduleDefinition(AdminModel):
    slug: str
    name: str
    schedule: Optional[str] = None
    timezone: Optional[str] = None
    environment: Optional[EnvironmentLink] = None
    git_branch: Optional[str] = None
    owner_email: Optional[str] = None
    description: Optional[str] = None
    sla_seconds: Optional[int] = None
    commands: Optional[List[str]] = None
    is_suspended: Optional[bool] = None
    trigger_on_merge: Optional[bool] = None
    trigger_on_webhook: Optional[bool] = None
    updated_at: Optional[datetime] = None


class TriggerUrl(AdminModel):
    public_id: str
    name: str
    is_enabled: Optional[bool] = None
    url: Optional[str] = None
    schedule: Optional[BoltScheduleLink] = None
    created_at: Optional[datetime] = None
    last_event_at: Optional[datetime] = None
    has_secret: Optional[bool] = None
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

from datetime import datetime
from typing import List, Optional

from paradime.apis.admin.types import (
    BOLT_SCHEDULE_LINK_FIELDS,
    ENVIRONMENT_LINK_FIELDS,
    AdminInputModel,
    AdminModel,
    BoltScheduleLink,
    EnvironmentLink,
    OpenEnum,
)

ENVIRONMENT_FIELDS = f"""
    slug
    type
    name
    isDefault
    branch
    dbtVersion
    createdAt
    deferEnvironment {{
        {ENVIRONMENT_LINK_FIELDS}
    }}
    deferSchedule {{
        {BOLT_SCHEDULE_LINK_FIELDS}
    }}
    hasWarehouseConnection
    variableNames
"""

ENVIRONMENT_LINKS = frozenset({"deferEnvironment", "deferSchedule"})


class EnvironmentType(OpenEnum):
    SCHEDULER = "SCHEDULER"
    AGENT = "AGENT"
    UNKNOWN = "UNKNOWN"


class Environment(AdminModel):
    slug: str
    type: EnvironmentType
    name: str
    is_default: Optional[bool] = None
    branch: Optional[str] = None
    dbt_version: Optional[str] = None
    created_at: Optional[datetime] = None
    defer_environment: Optional[EnvironmentLink] = None
    defer_schedule: Optional[BoltScheduleLink] = None
    has_warehouse_connection: Optional[bool] = None
    variable_names: Optional[List[str]] = None

    @property
    def defer_environment_slug(self) -> Optional[str]:
        return self.defer_environment.slug if self.defer_environment else None

    @property
    def defer_schedule_slug(self) -> Optional[str]:
        return self.defer_schedule.slug if self.defer_schedule else None


class CreateEnvironmentInput(AdminInputModel):
    type: EnvironmentType
    name: str
    slug: Optional[str] = None
    branch: Optional[str] = None
    dbt_version: Optional[str] = None


class UpdateEnvironmentInput(AdminInputModel):
    slug: str
    name: Optional[str] = None
    branch: Optional[str] = None
    dbt_version: Optional[str] = None


class DeleteEnvironmentInput(AdminInputModel):
    slug: str


class SetDefaultEnvironmentInput(AdminInputModel):
    slug: str


class DeferTargetInput(AdminInputModel):
    environment_slug: Optional[str] = None
    schedule_slug: Optional[str] = None


class SetAgentEnvironmentDeferTargetInput(AdminInputModel):
    slug: str
    defer_to: Optional[DeferTargetInput] = None

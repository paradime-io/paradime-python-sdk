from datetime import datetime
from typing import List, Optional

from paradime.apis.admin.types import AdminInputModel, AdminModel, OpenEnum

ENVIRONMENT_FIELDS = """
    slug
    type
    name
    isDefault
    branch
    dbtVersion
    createdAt
    deferEnvironmentSlug
    deferScheduleSlug
    hasWarehouseConnection
    variableNames
"""


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
    defer_environment_slug: Optional[str] = None
    defer_schedule_slug: Optional[str] = None
    has_warehouse_connection: Optional[bool] = None
    variable_names: Optional[List[str]] = None


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

from datetime import datetime
from typing import List, Optional

from paradime.apis.admin.types import AdminInputModel, AdminModel, OpenEnum


class EnvironmentVariableScope(OpenEnum):
    WORKSPACE = "WORKSPACE"
    ENVIRONMENT = "ENVIRONMENT"
    UNKNOWN = "UNKNOWN"


class SetEnvironmentVariablesMode(OpenEnum):
    MERGE = "MERGE"
    REPLACE = "REPLACE"
    UNKNOWN = "UNKNOWN"


class EnvironmentVariable(AdminModel):
    name: str
    is_set: bool
    updated_at: Optional[datetime] = None


class SetEnvironmentVariablesResult(AdminModel):
    changed_names: List[str]
    total_count: int


class EnvironmentVariableValueInput(AdminInputModel):
    name: str
    value: str


class SetEnvironmentVariablesInput(AdminInputModel):
    scope: EnvironmentVariableScope
    environment_slug: Optional[str] = None
    variables: List[EnvironmentVariableValueInput]
    mode: Optional[SetEnvironmentVariablesMode] = None


class DeleteEnvironmentVariablesInput(AdminInputModel):
    scope: EnvironmentVariableScope
    environment_slug: Optional[str] = None
    names: List[str]


class RenameEnvironmentVariableInput(AdminInputModel):
    scope: EnvironmentVariableScope
    environment_slug: Optional[str] = None
    name: str
    new_name: str


class RevealEnvironmentVariableInput(AdminInputModel):
    scope: EnvironmentVariableScope
    environment_slug: Optional[str] = None
    name: str

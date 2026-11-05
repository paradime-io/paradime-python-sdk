from datetime import datetime
from typing import List, Optional

from paradime.apis.admin.types import AdminInputModel, AdminModel

WORKSPACE_FIELDS = """
    uid
    name
    createdAt
    dbtVersion
    supportedDbtVersions
    dbtPartialParsing
    defaultBranch
    scheduleDeploymentBranch
"""


class AdminWorkspace(AdminModel):
    uid: str
    name: str
    created_at: Optional[datetime] = None
    dbt_version: Optional[str] = None
    supported_dbt_versions: Optional[List[str]] = None
    dbt_partial_parsing: Optional[bool] = None
    default_branch: Optional[str] = None
    schedule_deployment_branch: Optional[str] = None


class CreateWorkspaceInput(AdminInputModel):
    name: str
    dbt_version: Optional[str] = None


class UpdateWorkspaceInput(AdminInputModel):
    workspace_uid: str
    name: Optional[str] = None
    dbt_version: Optional[str] = None
    dbt_partial_parsing: Optional[bool] = None
    default_branch: Optional[str] = None
    schedule_deployment_branch: Optional[str] = None


class DeleteWorkspaceInput(AdminInputModel):
    workspace_uid: str

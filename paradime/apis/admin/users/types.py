from datetime import datetime
from typing import List, Optional

from paradime.apis.admin.types import AdminInputModel, AdminModel, OpenEnum

USER_FIELDS = """
    email
    uid
    fullName
    accountType
    status
    inviteStatus
    invitedAt
    lastLoginAt
"""


class UserStatus(OpenEnum):
    ACTIVE = "ACTIVE"
    INVITED = "INVITED"
    DEACTIVATED = "DEACTIVATED"
    UNKNOWN = "UNKNOWN"


class UserInviteStatus(OpenEnum):
    PENDING = "PENDING"
    EXPIRED = "EXPIRED"
    UNKNOWN = "UNKNOWN"


class AdminUser(AdminModel):
    email: Optional[str] = None
    uid: Optional[str] = None
    full_name: Optional[str] = None
    account_type: Optional[str] = None
    status: UserStatus
    invite_status: Optional[UserInviteStatus] = None
    invited_at: Optional[datetime] = None
    last_login_at: Optional[datetime] = None


class ReactivateUserInput(AdminInputModel):
    uid: str


class SetAutoJoinDomainsInput(AdminInputModel):
    domains: List[str]

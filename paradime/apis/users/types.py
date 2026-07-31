from enum import Enum

from paradime.tools.models import ParadimeResponseModel


class UserAccountType(str, Enum):
    ADMIN = "ADMIN"
    DEVELOPER = "DEVELOPER"
    BUSINESS = "BUSINESS"
    BILLING_ADMIN = "BILLING_ADMIN"
    SECURITY_ADMIN = "SECURITY_ADMIN"
    WORKSPACE_SETTINGS_ADMIN = "WORKSPACE_SETTINGS_ADMIN"
    WORKSPACE_ADMIN = "WORKSPACE_ADMIN"
    ANALYST = "ANALYST"
    SANDBOX_USER = "SANDBOX_USER"


class InviteStatus(str, Enum):
    SENT = "SENT"
    EXPIRED = "EXPIRED"


class ActiveUser(ParadimeResponseModel):
    uid: str
    email: str
    name: str
    account_type: str


class InvitedUser(ParadimeResponseModel):
    email: str
    account_type: str
    invite_status: str

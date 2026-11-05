from datetime import datetime
from typing import List, Optional

from paradime.apis.admin.types import AdminInputModel, AdminModel, OpenEnum

SLACK_INSTALLATION_FIELDS = """
    status
    installUrl
    teamId
    teamName
    reconnectRequired
    missingScopes
"""

ALERT_NOTIFICATIONS_FIELDS = """
    category
    slack {
        channelIds
        userIds
    }
    msTeams {
        webhookNames
    }
    email {
        enabled
    }
"""

MS_TEAMS_WEBHOOK_FIELDS = """
    name
    urlIsSet
    urlHint
    updatedAt
"""


class AlertCategory(OpenEnum):
    BOLT_SYSTEM = "BOLT_SYSTEM"
    UNKNOWN = "UNKNOWN"


class SlackInstallationStatus(OpenEnum):
    CONNECTED = "CONNECTED"
    PENDING_USER_ACTION = "PENDING_USER_ACTION"
    UNKNOWN = "UNKNOWN"


class SlackInstallation(AdminModel):
    status: SlackInstallationStatus
    install_url: Optional[str] = None
    team_id: Optional[str] = None
    team_name: Optional[str] = None
    reconnect_required: Optional[bool] = None
    missing_scopes: Optional[List[str]] = None


class SlackAlertDestination(AdminModel):
    channel_ids: List[str]
    user_ids: List[str]


class MsTeamsAlertDestination(AdminModel):
    webhook_names: List[str]


class EmailAlertDestination(AdminModel):
    enabled: Optional[bool] = None


class AlertNotifications(AdminModel):
    category: AlertCategory
    slack: Optional[SlackAlertDestination] = None
    ms_teams: Optional[MsTeamsAlertDestination] = None
    email: Optional[EmailAlertDestination] = None


class MsTeamsWebhook(AdminModel):
    name: str
    url_is_set: bool
    url_hint: Optional[str] = None
    updated_at: Optional[datetime] = None


class DisconnectSlackInput(AdminInputModel):
    team_id: Optional[str] = None


class SetMsTeamsWebhookInput(AdminInputModel):
    name: str
    url: str


class DeleteMsTeamsWebhookInput(AdminInputModel):
    name: str


class SlackAlertDestinationInput(AdminInputModel):
    channel_ids: List[str] = []
    user_ids: List[str] = []


class MsTeamsAlertDestinationInput(AdminInputModel):
    webhook_names: List[str]


class EmailAlertDestinationInput(AdminInputModel):
    enabled: bool


class SetAlertNotificationsInput(AdminInputModel):
    category: AlertCategory
    slack: Optional[SlackAlertDestinationInput] = None
    ms_teams: Optional[MsTeamsAlertDestinationInput] = None
    email: Optional[EmailAlertDestinationInput] = None

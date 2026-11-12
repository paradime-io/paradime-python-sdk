from typing import Any, Dict, Optional, Sequence

from paradime.apis.admin.notifications.types import (
    ALERT_NOTIFICATIONS_FIELDS,
    SLACK_INSTALLATION_FIELDS,
    AlertCategory,
    AlertNotifications,
    DisconnectSlackInput,
    EmailAlertDestinationInput,
    SetAlertNotificationsInput,
    SlackAlertDestinationInput,
    SlackInstallation,
)
from paradime.apis.admin.transport import AdminTransport
from paradime.apis.admin.types import serialize_input


class AdminNotificationsClient:
    """
    Where a workspace's alerts go: Slack and email.
    """

    def __init__(self, transport: AdminTransport) -> None:
        self.transport = transport

    def get_slack(self, *, workspace_uid: Optional[str] = None) -> Optional[SlackInstallation]:
        """
        Reads the account's Slack connection. When Slack is not connected, `install_url` is
        the page where an account admin connects it in a browser.

        Args:
            workspace_uid (str, optional): The workspace to target, for a company-level key.

        Returns:
            Optional[SlackInstallation]: The connection, or None when the server returned none.
        """

        query = f"""
            query adminSlack {{
                slack {{
                    {SLACK_INSTALLATION_FIELDS}
                }}
            }}
        """

        response = self.transport.execute(query, {}, workspace_uid=workspace_uid)
        slack = response["slack"]
        return SlackInstallation.parse_obj(slack) if slack is not None else None

    def disconnect_slack(
        self, *, team_id: Optional[str] = None, workspace_uid: Optional[str] = None
    ) -> SlackInstallation:
        """
        Removes the account's Slack connection. Disconnecting again is not an error.

        Args:
            team_id (str, optional): The Slack team expected to be connected; when another
                team is, nothing changes and the call fails with `CONFLICT`.
            workspace_uid (str, optional): The workspace to target, for a company-level key.

        Returns:
            SlackInstallation: The connection, now pending.
        """

        query = f"""
            mutation adminDisconnectSlack($input: DisconnectSlackInput!) {{
                disconnectSlack(input: $input) {{
                    slack {{
                        {SLACK_INSTALLATION_FIELDS}
                    }}
                }}
            }}
        """

        disconnect_input = (
            DisconnectSlackInput(team_id=team_id) if team_id else DisconnectSlackInput()
        )
        response = self.transport.execute(
            query, {"input": serialize_input(disconnect_input)}, workspace_uid=workspace_uid
        )
        return SlackInstallation.parse_obj(response["disconnectSlack"]["slack"])

    def get_alert_notifications(
        self,
        category: AlertCategory = AlertCategory.BOLT_SYSTEM,
        *,
        workspace_uid: Optional[str] = None,
    ) -> Optional[AlertNotifications]:
        """
        Reads where one category of the workspace's alerts goes.

        Args:
            category (AlertCategory): The alerts to read; `BOLT_SYSTEM` by default.
            workspace_uid (str, optional): The workspace to read, for a company-level key.

        Returns:
            Optional[AlertNotifications]: The destinations, or None when the server returned none.
        """

        query = f"""
            query adminAlertNotifications($workspaceUid: String, $category: AlertCategory!) {{
                workspace(uid: $workspaceUid) {{
                    alertNotifications(category: $category) {{
                        {ALERT_NOTIFICATIONS_FIELDS}
                    }}
                }}
            }}
        """

        response = self.transport.execute(
            query,
            {"workspaceUid": workspace_uid, "category": AlertCategory(category).value},
            workspace_uid=workspace_uid,
        )
        notifications = (response["workspace"] or {}).get("alertNotifications")
        return AlertNotifications.parse_obj(notifications) if notifications is not None else None

    def set_alert_notifications(
        self,
        category: AlertCategory = AlertCategory.BOLT_SYSTEM,
        *,
        slack_channel_ids: Optional[Sequence[str]] = None,
        slack_user_ids: Optional[Sequence[str]] = None,
        email_enabled: Optional[bool] = None,
        workspace_uid: Optional[str] = None,
    ) -> AlertNotifications:
        """
        Routes one category of the workspace's alerts. A destination left out keeps its value;
        each one given replaces its value, so an empty list removes it.

        Args:
            category (AlertCategory): The alerts to route; `BOLT_SYSTEM` by default.
            slack_channel_ids (Sequence[str], optional): Slack channel ids. Given with or
                without `slack_user_ids`, it replaces the Slack destination.
            slack_user_ids (Sequence[str], optional): Slack user ids.
            email_enabled (bool, optional): Whether alert emails are on.
            workspace_uid (str, optional): The workspace to change, for a company-level key.

        Returns:
            AlertNotifications: Where the category's alerts now go.
        """

        query = f"""
            mutation adminSetAlertNotifications($input: SetAlertNotificationsInput!) {{
                setAlertNotifications(input: $input) {{
                    alertNotifications {{
                        {ALERT_NOTIFICATIONS_FIELDS}
                    }}
                }}
            }}
        """

        destinations: Dict[str, Any] = {}
        if slack_channel_ids is not None or slack_user_ids is not None:
            destinations["slack"] = SlackAlertDestinationInput(
                channel_ids=list(slack_channel_ids or []), user_ids=list(slack_user_ids or [])
            )
        if email_enabled is not None:
            destinations["email"] = EmailAlertDestinationInput(enabled=email_enabled)
        notifications_input = SetAlertNotificationsInput(category=category, **destinations)
        response = self.transport.execute(
            query, {"input": serialize_input(notifications_input)}, workspace_uid=workspace_uid
        )
        return AlertNotifications.parse_obj(response["setAlertNotifications"]["alertNotifications"])

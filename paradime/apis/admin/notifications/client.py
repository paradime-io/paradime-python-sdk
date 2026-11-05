from typing import Any, Dict, Iterator, Optional, Sequence

from paradime.apis.admin.notifications.types import (
    ALERT_NOTIFICATIONS_FIELDS,
    MS_TEAMS_WEBHOOK_FIELDS,
    SLACK_INSTALLATION_FIELDS,
    AlertCategory,
    AlertNotifications,
    DeleteMsTeamsWebhookInput,
    DisconnectSlackInput,
    EmailAlertDestinationInput,
    MsTeamsAlertDestinationInput,
    MsTeamsWebhook,
    SetAlertNotificationsInput,
    SetMsTeamsWebhookInput,
    SlackAlertDestinationInput,
    SlackInstallation,
)
from paradime.apis.admin.transport import AdminTransport
from paradime.apis.admin.types import (
    CONNECTION_FIELDS,
    DEFAULT_PAGE_SIZE,
    Page,
    check_page_size,
    iterate_pages,
    parse_page,
    serialize_input,
)


class AdminNotificationsClient:
    """
    Where a workspace's alerts go: Slack, Microsoft Teams webhooks and email.
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

    def list_ms_teams_webhooks(
        self, *, workspace_uid: Optional[str] = None, page_size: int = DEFAULT_PAGE_SIZE
    ) -> Iterator[MsTeamsWebhook]:
        """
        Iterates over the workspace's Microsoft Teams webhooks, ordered by name. Their URLs are
        never returned.

        Args:
            workspace_uid (str, optional): The workspace to read, for a company-level key.
            page_size (int): How many webhooks to read per request, at most 500.

        Returns:
            Iterator[MsTeamsWebhook]: The webhooks, read a page at a time.
        """

        check_page_size(page_size)
        query = f"""
            query adminMsTeamsWebhooks($workspaceUid: String, $first: Int, $after: String) {{
                workspace(uid: $workspaceUid) {{
                    msTeamsWebhooks(first: $first, after: $after) {{
                        edges {{
                            node {{
                                {MS_TEAMS_WEBHOOK_FIELDS}
                            }}
                        }}
                        {CONNECTION_FIELDS}
                    }}
                }}
            }}
        """

        def fetch_page(after: Optional[str]) -> Page[MsTeamsWebhook]:
            response = self.transport.execute(
                query,
                {"workspaceUid": workspace_uid, "first": page_size, "after": after},
                workspace_uid=workspace_uid,
            )
            workspace = response["workspace"] or {}
            return parse_page(workspace.get("msTeamsWebhooks"), MsTeamsWebhook.parse_obj)

        return iterate_pages(fetch_page)

    def set_ms_teams_webhook(
        self, name: str, url: str, *, workspace_uid: Optional[str] = None
    ) -> MsTeamsWebhook:
        """
        Adds a Microsoft Teams webhook, or changes one by name. The URL is stored as a secret.

        Args:
            name (str): The webhook's name, unique in its workspace.
            url (str): The webhook's https URL.
            workspace_uid (str, optional): The workspace to change, for a company-level key.

        Returns:
            MsTeamsWebhook: The webhook.
        """

        query = f"""
            mutation adminSetMsTeamsWebhook($input: SetMsTeamsWebhookInput!) {{
                setMsTeamsWebhook(input: $input) {{
                    msTeamsWebhook {{
                        {MS_TEAMS_WEBHOOK_FIELDS}
                    }}
                }}
            }}
        """

        webhook_input = SetMsTeamsWebhookInput(name=name, url=url)
        response = self.transport.execute(
            query, {"input": serialize_input(webhook_input)}, workspace_uid=workspace_uid
        )
        return MsTeamsWebhook.parse_obj(response["setMsTeamsWebhook"]["msTeamsWebhook"])

    def delete_ms_teams_webhook(self, name: str, *, workspace_uid: Optional[str] = None) -> str:
        """
        Deletes a Microsoft Teams webhook. Deleting it again is not an error.

        Args:
            name (str): The webhook's name.
            workspace_uid (str, optional): The workspace to change, for a company-level key.

        Returns:
            str: The name of the deleted webhook.
        """

        query = """
            mutation adminDeleteMsTeamsWebhook($input: DeleteMsTeamsWebhookInput!) {
                deleteMsTeamsWebhook(input: $input) {
                    deletedName
                }
            }
        """

        response = self.transport.execute(
            query,
            {"input": serialize_input(DeleteMsTeamsWebhookInput(name=name))},
            workspace_uid=workspace_uid,
        )
        return response["deleteMsTeamsWebhook"]["deletedName"]

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
        ms_teams_webhook_names: Optional[Sequence[str]] = None,
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
            ms_teams_webhook_names (Sequence[str], optional): Names of the workspace's webhooks.
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
        if ms_teams_webhook_names is not None:
            destinations["ms_teams"] = MsTeamsAlertDestinationInput(
                webhook_names=list(ms_teams_webhook_names)
            )
        if email_enabled is not None:
            destinations["email"] = EmailAlertDestinationInput(enabled=email_enabled)
        notifications_input = SetAlertNotificationsInput(category=category, **destinations)
        response = self.transport.execute(
            query, {"input": serialize_input(notifications_input)}, workspace_uid=workspace_uid
        )
        return AlertNotifications.parse_obj(response["setAlertNotifications"]["alertNotifications"])

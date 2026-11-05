from typing import Optional

from paradime.apis.admin.bolt.client import AdminBoltClient
from paradime.apis.admin.connections.client import AdminConnectionsClient
from paradime.apis.admin.dinoai.client import AdminDinoaiClient
from paradime.apis.admin.environment_variables.client import AdminEnvironmentVariablesClient
from paradime.apis.admin.environments.client import AdminEnvironmentsClient
from paradime.apis.admin.git.client import AdminGitClient
from paradime.apis.admin.integrations.client import AdminIntegrationsClient
from paradime.apis.admin.jobs.client import AdminJobsClient
from paradime.apis.admin.notifications.client import AdminNotificationsClient
from paradime.apis.admin.transport import AdminTransport
from paradime.apis.admin.usage.client import AdminUsageClient
from paradime.apis.admin.users.client import AdminUsersClient
from paradime.apis.admin.workspaces.client import AdminWorkspacesClient
from paradime.client.api_client import APIClient


class AdminClient:
    """
    The Paradime Administration API: workspaces, git, warehouse connections, environments,
    environment variables, usage, users, notifications, Bolt, integrations, DinoAI and async
    jobs.

    The Admin API is available on the Enterprise plan. Errors raise the `AdminApiException`
    subclass of their `extensions.code`, and rate-limited requests are retried after the wait
    the server asks for. Workspace-level operations take `workspace_uid` to target a workspace
    with a company-level key for one call, in place of the client's `workspace_uid`.

    Attributes:
        workspaces (AdminWorkspacesClient): Workspaces and their settings.
        git (AdminGitClient): The workspace's git repository and deploy key.
        connections (AdminConnectionsClient): Data warehouse connections.
        environments (AdminEnvironmentsClient): Bolt and agent environments.
        environment_variables (AdminEnvironmentVariablesClient): Environment variables.
        usage (AdminUsageClient): Bolt and DinoAI usage.
        jobs (AdminJobsClient): Async jobs, and waiting for them.
        users (AdminUsersClient): Users and auto-join domains.
        notifications (AdminNotificationsClient): Slack, Microsoft Teams and email alerts.
        bolt (AdminBoltClient): Schedule updates and deploys, and Trigger URLs.
        integrations (AdminIntegrationsClient): Integrations and external secret managers.
        dinoai (AdminDinoaiClient): DinoAI agent definitions and settings.
    """

    def __init__(self, client: APIClient, *, transport: Optional[AdminTransport] = None) -> None:
        self.transport = transport or AdminTransport(client)
        self.workspaces = AdminWorkspacesClient(self.transport)
        self.git = AdminGitClient(self.transport)
        self.connections = AdminConnectionsClient(self.transport)
        self.environments = AdminEnvironmentsClient(self.transport)
        self.environment_variables = AdminEnvironmentVariablesClient(self.transport)
        self.usage = AdminUsageClient(self.transport)
        self.jobs = AdminJobsClient(self.transport)
        self.users = AdminUsersClient(self.transport)
        self.notifications = AdminNotificationsClient(self.transport)
        self.bolt = AdminBoltClient(self.transport)
        self.integrations = AdminIntegrationsClient(self.transport)
        self.dinoai = AdminDinoaiClient(self.transport)

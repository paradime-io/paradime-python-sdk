from typing import Optional, Union

from paradime.apis.admin.git.types import (
    GIT_REPOSITORY_FIELDS,
    CheckGitRepositoryInput,
    ConnectGitRepositoryInput,
    ConnectGitRepositoryResult,
    DeployKeyConnectionInput,
    GitConnectionInput,
    GitDeployKey,
    GitHubAppConnectionInput,
    GitLabAppConnectionInput,
    GitRepository,
    RotateGitDeployKeyInput,
)
from paradime.apis.admin.jobs.types import ADMIN_JOB_FIELDS, AdminJob
from paradime.apis.admin.transport import AdminTransport
from paradime.apis.admin.types import serialize_input

GitConnection = Union[DeployKeyConnectionInput, GitHubAppConnectionInput, GitLabAppConnectionInput]


def _connection_input(connection: GitConnection) -> GitConnectionInput:
    if isinstance(connection, DeployKeyConnectionInput):
        return GitConnectionInput(deploy_key=connection)
    if isinstance(connection, GitHubAppConnectionInput):
        return GitConnectionInput(github_app=connection)
    if isinstance(connection, GitLabAppConnectionInput):
        return GitConnectionInput(gitlab_app=connection)
    raise TypeError(f"Unsupported git connection: {type(connection).__name__}")


class AdminGitClient:
    def __init__(self, transport: AdminTransport) -> None:
        self.transport = transport

    def get_repository(self, *, workspace_uid: Optional[str] = None) -> Optional[GitRepository]:
        """
        Reads the workspace's git repository and how Paradime reaches it.

        A key with `git:write` creates the workspace's first deploy key when it has none.

        Args:
            workspace_uid (str, optional): The workspace to read, for a company-level key.

        Returns:
            Optional[GitRepository]: The repository, or None when the server returned none.
        """

        query = f"""
            query adminGitRepository($workspaceUid: String) {{
                workspace(uid: $workspaceUid) {{
                    gitRepository {{
                        {GIT_REPOSITORY_FIELDS}
                    }}
                }}
            }}
        """

        response = self.transport.execute(
            query, {"workspaceUid": workspace_uid}, workspace_uid=workspace_uid
        )
        repository = (response["workspace"] or {}).get("gitRepository")
        return GitRepository.parse_obj(repository) if repository is not None else None

    def connect(
        self, connection: GitConnection, *, workspace_uid: Optional[str] = None
    ) -> ConnectGitRepositoryResult:
        """
        Connects the workspace's repository. Sending the same input again changes nothing.

        Args:
            connection: How to reach the repository: a `DeployKeyConnectionInput`, a
                `GitHubAppConnectionInput` or a `GitLabAppConnectionInput`.
            workspace_uid (str, optional): The workspace to change, for a company-level key.

        Returns:
            ConnectGitRepositoryResult: The repository now and, when the deploy key connects it
                with validation, the cloning job; wait for it with `admin.jobs.wait`.
        """

        query = f"""
            mutation adminConnectGitRepository($input: ConnectGitRepositoryInput!) {{
                connectGitRepository(input: $input) {{
                    gitRepository {{
                        {GIT_REPOSITORY_FIELDS}
                    }}
                    job {{
                        {ADMIN_JOB_FIELDS}
                    }}
                }}
            }}
        """

        connect_input = ConnectGitRepositoryInput(connection=_connection_input(connection))
        response = self.transport.execute(
            query, {"input": serialize_input(connect_input)}, workspace_uid=workspace_uid
        )
        return ConnectGitRepositoryResult.parse_obj(response["connectGitRepository"])

    def check_repository(self, uri: str, *, workspace_uid: Optional[str] = None) -> AdminJob:
        """
        Starts a job that checks the workspace's deploy key can push to a repository. The job
        fails with `CONNECTION_TEST_FAILED` when it can't. Changes nothing.

        Args:
            uri (str): The repository's SSH URI.
            workspace_uid (str, optional): The workspace to use, for a company-level key.

        Returns:
            AdminJob: The job; wait for it with `admin.jobs.wait`.
        """

        query = f"""
            mutation adminCheckGitRepository($input: CheckGitRepositoryInput!) {{
                checkGitRepository(input: $input) {{
                    job {{
                        {ADMIN_JOB_FIELDS}
                    }}
                }}
            }}
        """

        check_input = CheckGitRepositoryInput(uri=uri)
        response = self.transport.execute(
            query, {"input": serialize_input(check_input)}, workspace_uid=workspace_uid
        )
        return AdminJob.parse_obj(response["checkGitRepository"]["job"])

    def rotate_deploy_key(
        self, *, force: bool = False, workspace_uid: Optional[str] = None
    ) -> GitDeployKey:
        """
        Replaces the workspace's deploy key.

        Args:
            force (bool): Rotate a key the connected repository uses. Clone and push break
                until the new key is added to the repository.
            workspace_uid (str, optional): The workspace to change, for a company-level key.

        Returns:
            GitDeployKey: The new deploy key.
        """

        query = """
            mutation adminRotateGitDeployKey($input: RotateGitDeployKeyInput!) {
                rotateGitDeployKey(input: $input) {
                    deployKey {
                        publicKey
                    }
                }
            }
        """

        rotate_input = RotateGitDeployKeyInput(force=force) if force else RotateGitDeployKeyInput()
        response = self.transport.execute(
            query, {"input": serialize_input(rotate_input)}, workspace_uid=workspace_uid
        )
        return GitDeployKey.parse_obj(response["rotateGitDeployKey"]["deployKey"])

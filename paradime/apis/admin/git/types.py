from typing import Optional

from paradime.apis.admin.jobs.types import AdminJob
from paradime.apis.admin.types import AdminInputModel, AdminModel, OpenEnum

GIT_REPOSITORY_FIELDS = """
    mode
    state
    provider
    uri
    dbtProjectDir
    verified
    deployKey {
        publicKey
    }
    githubApp {
        installationId
        repositoryId
        repositoryFullName
    }
    gitlabApp {
        projectId
        projectPath
    }
    githubAppInstallUrl
"""


class GitProvider(OpenEnum):
    GITHUB = "GITHUB"
    GITLAB = "GITLAB"
    BITBUCKET = "BITBUCKET"
    AZURE_DEVOPS = "AZURE_DEVOPS"
    UNKNOWN = "UNKNOWN"


class GitRepositoryMode(OpenEnum):
    SSH_DEPLOY_KEY = "SSH_DEPLOY_KEY"
    GITHUB_APP = "GITHUB_APP"
    GITLAB_APP = "GITLAB_APP"
    UNKNOWN = "UNKNOWN"


class GitRepositoryState(OpenEnum):
    NOT_CONNECTED = "NOT_CONNECTED"
    PENDING_USER_ACTION = "PENDING_USER_ACTION"
    PENDING_VALIDATION = "PENDING_VALIDATION"
    CONNECTED = "CONNECTED"
    DEGRADED = "DEGRADED"
    DISCONNECTED = "DISCONNECTED"
    UNKNOWN = "UNKNOWN"


class GitDeployKey(AdminModel):
    public_key: str


class GitHubAppRepository(AdminModel):
    installation_id: Optional[str] = None
    repository_id: Optional[str] = None
    repository_full_name: Optional[str] = None


class GitLabAppRepository(AdminModel):
    project_id: Optional[str] = None
    project_path: Optional[str] = None


class GitRepository(AdminModel):
    mode: GitRepositoryMode
    state: GitRepositoryState
    provider: Optional[GitProvider] = None
    uri: Optional[str] = None
    dbt_project_dir: Optional[str] = None
    verified: Optional[bool] = None
    deploy_key: Optional[GitDeployKey] = None
    github_app: Optional[GitHubAppRepository] = None
    gitlab_app: Optional[GitLabAppRepository] = None
    github_app_install_url: Optional[str] = None


class ConnectGitRepositoryResult(AdminModel):
    git_repository: GitRepository
    job: Optional[AdminJob] = None


class DeployKeyConnectionInput(AdminInputModel):
    provider: GitProvider
    uri: str
    dbt_project_dir: Optional[str] = None
    validate_repository: Optional[bool] = None
    force: Optional[bool] = None


class GitHubAppConnectionInput(AdminInputModel):
    installation_id: Optional[str] = None
    repository_id: Optional[str] = None


class GitLabAppConnectionInput(AdminInputModel):
    base_url: Optional[str] = None
    project_id: Optional[str] = None


class GitConnectionInput(AdminInputModel):
    deploy_key: Optional[DeployKeyConnectionInput] = None
    github_app: Optional[GitHubAppConnectionInput] = None
    gitlab_app: Optional[GitLabAppConnectionInput] = None


class ConnectGitRepositoryInput(AdminInputModel):
    connection: GitConnectionInput


class CheckGitRepositoryInput(AdminInputModel):
    uri: str


class RotateGitDeployKeyInput(AdminInputModel):
    force: Optional[bool] = None

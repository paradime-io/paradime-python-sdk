from typing import List, Optional

from paradime.apis.admin.integrations.types import AgentIntegrationName
from paradime.apis.admin.types import AdminModel, OpenEnum

PRODUCT_READINESS_FIELDS = """
    status
    missing
    missingSuggested
"""

WORKSPACE_SETUP_STATUS_FIELDS = f"""
    isComplete
    steps {{
        step
        status
        isRequired
        environmentSlug
        agentIntegration
        installUrl
        detail
    }}
    products {{
        codeIde {{
            {PRODUCT_READINESS_FIELDS}
        }}
        bolt {{
            {PRODUCT_READINESS_FIELDS}
        }}
        agent {{
            {PRODUCT_READINESS_FIELDS}
        }}
    }}
"""


class SetupStep(OpenEnum):
    GIT_REPOSITORY = "GIT_REPOSITORY"
    GITHUB_APP = "GITHUB_APP"
    DEVELOPMENT_CONNECTION = "DEVELOPMENT_CONNECTION"
    SCHEDULER_CONNECTION = "SCHEDULER_CONNECTION"
    AGENT_CONNECTION = "AGENT_CONNECTION"
    AGENT_INTEGRATION = "AGENT_INTEGRATION"
    ENVIRONMENT_VARIABLES = "ENVIRONMENT_VARIABLES"
    SCHEDULES_DEPLOYED = "SCHEDULES_DEPLOYED"
    NOTIFICATIONS = "NOTIFICATIONS"
    FIRST_BOLT_RUN = "FIRST_BOLT_RUN"
    UNKNOWN = "UNKNOWN"


class SetupStepStatus(OpenEnum):
    DONE = "DONE"
    MISSING = "MISSING"
    PENDING_USER_ACTION = "PENDING_USER_ACTION"
    UNKNOWN = "UNKNOWN"


class WorkspaceSetupStep(AdminModel):
    step: SetupStep
    status: SetupStepStatus
    is_required: bool
    environment_slug: Optional[str] = None
    agent_integration: Optional[AgentIntegrationName] = None
    install_url: Optional[str] = None
    detail: Optional[str] = None


class ProductReadiness(AdminModel):
    status: SetupStepStatus
    missing: List[SetupStep]
    missing_suggested: List[SetupStep]


class WorkspaceProducts(AdminModel):
    code_ide: ProductReadiness
    bolt: ProductReadiness
    agent: ProductReadiness


class WorkspaceSetupStatus(AdminModel):
    is_complete: bool
    steps: Optional[List[WorkspaceSetupStep]] = None
    products: WorkspaceProducts

from datetime import datetime

from paradime.apis.admin.types import AdminInputModel, AdminModel

USAGE_TOTALS_FIELDS = """
    dinoai {
        credits
        backgroundAgentCredits
    }
    bolt {
        billedSeconds
    }
"""


class DinoAiUsage(AdminModel):
    credits: int
    background_agent_credits: int


class BoltUsage(AdminModel):
    billed_seconds: int


class Usage(AdminModel):
    period_from: datetime
    period_to: datetime
    dinoai: DinoAiUsage
    bolt: BoltUsage


class UsageWorkspace(AdminModel):
    uid: str
    name: str


class WorkspaceUsage(AdminModel):
    workspace: UsageWorkspace
    dinoai: DinoAiUsage
    bolt: BoltUsage


class UsagePeriodInput(AdminInputModel):
    period_from: datetime
    period_to: datetime

from typing import List, Optional

import click

from paradime.apis.admin.exception import AdminNotFoundException
from paradime.apis.admin.setup_status.types import ProductReadiness, SetupStepStatus
from paradime.cli import console
from paradime.cli.admin import common


def _details(detail: Optional[str], install_url: Optional[str], status: SetupStepStatus) -> str:
    parts: List[str] = [detail] if detail else []
    if install_url and status == SetupStepStatus.PENDING_USER_ACTION:
        parts.append(f"Finish it at {install_url}")
    return " ".join(parts)


def _readiness(readiness: ProductReadiness) -> str:
    text = readiness.status.value
    if readiness.missing:
        text += f"; needs {', '.join(step.value for step in readiness.missing)}"
    if readiness.missing_suggested:
        text += f"; better with {', '.join(step.value for step in readiness.missing_suggested)}"
    return text


@click.command(name="setup-status")
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def setup_status(workspace_uid: Optional[str], as_json: bool) -> None:
    """
    Show a workspace's onboarding checklist: each step, where it is, and the page to finish
    the steps that wait for a person in a browser.
    """
    status = common.get_admin().setup_status.get(workspace_uid=workspace_uid)
    if status is None:
        raise AdminNotFoundException("No setup status for this workspace.")
    if as_json:
        console.json_out(common.to_json(status))
        return
    console.table(
        ["Step", "Environment", "Status", "Required", "Details"],
        [
            [
                step.step.value,
                step.environment_slug or "",
                step.status.value,
                common.show(step.required),
                _details(step.detail, step.install_url, step.status),
            ]
            for step in status.steps or []
        ],
        title="Setup complete" if status.is_complete else "Setup not complete",
    )
    console.kv("Code IDE", _readiness(status.products.code_ide))
    console.kv("Bolt", _readiness(status.products.bolt))
    console.kv("DinoAI agents", _readiness(status.products.agent))

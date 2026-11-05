from datetime import datetime
from typing import Optional

import click

from paradime.cli import console
from paradime.cli.admin import common

DATE_FORMATS = ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"]


@click.command()
@click.option(
    "--from", "period_from", type=click.DateTime(DATE_FORMATS), required=True, help="Start, UTC."
)
@click.option(
    "--to",
    "period_to",
    type=click.DateTime(DATE_FORMATS),
    required=True,
    help="End, excluded, UTC.",
)
@click.option("--workspace", "workspace_uid", default=None, help="Only this workspace's usage.")
@click.option("--per-workspace", is_flag=True, help="One row per workspace.")
@common.json_option
@common.handle_admin_errors
def usage(
    period_from: datetime,
    period_to: datetime,
    workspace_uid: Optional[str],
    per_workspace: bool,
    as_json: bool,
) -> None:
    """
    Show Bolt and DinoAI usage in a period, as the Usage page does.

    \b
    Example, the usage of October 2026:
      paradime admin usage --from 2026-10-01 --to 2026-11-01
    """
    admin = common.get_admin()
    if per_workspace:
        rows = list(
            admin.usage.list_workspaces(period_from, period_to, workspace_uid=workspace_uid)
        )
        if as_json:
            console.json_out([common.to_json(row) for row in rows])
            return
        console.table(
            ["Workspace", "UID", "DinoAI credits", "Agent credits", "Bolt minutes"],
            [
                [
                    row.workspace.name,
                    row.workspace.uid,
                    str(row.dinoai.credits),
                    str(row.dinoai.background_agent_credits),
                    f"{row.bolt.billed_seconds / 60:.1f}",
                ]
                for row in rows
            ],
        )
        return
    total = admin.usage.get(period_from, period_to, workspace_uid=workspace_uid)
    if total is None:
        console.info("No usage returned.")
        return
    common.print_model(total, as_json=as_json)

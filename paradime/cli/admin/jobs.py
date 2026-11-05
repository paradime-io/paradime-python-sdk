from typing import Optional

import click

from paradime.apis.admin.exception import AdminNotFoundException
from paradime.cli.admin import common


@click.group()
def jobs() -> None:
    """
    Follow the jobs async Admin operations start.
    """
    pass


@jobs.command()
@click.argument("job_id")
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def get(job_id: str, workspace_uid: Optional[str], as_json: bool) -> None:
    """
    Show a job.
    """
    job = common.get_admin().jobs.get(job_id, workspace_uid=workspace_uid)
    if job is None:
        raise AdminNotFoundException(f"No admin job with id {job_id!r}.")
    common.print_model(job, as_json=as_json)


@jobs.command()
@click.argument("job_id")
@click.option("--timeout", type=float, default=600, show_default=True, help="Seconds to wait.")
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def wait(job_id: str, timeout: float, workspace_uid: Optional[str], as_json: bool) -> None:
    """
    Wait for a job to finish. Exits non-zero when it fails or times out.
    """
    job = common.get_admin().jobs.wait(job_id, timeout=timeout, workspace_uid=workspace_uid)
    common.print_model(job, as_json=as_json)

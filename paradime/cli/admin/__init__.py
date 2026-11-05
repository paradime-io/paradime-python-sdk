import click

from paradime.cli.admin.connections import connections
from paradime.cli.admin.env_vars import env_vars
from paradime.cli.admin.environments import environments
from paradime.cli.admin.git import git
from paradime.cli.admin.jobs import jobs
from paradime.cli.admin.setup_status import setup_status
from paradime.cli.admin.usage import usage
from paradime.cli.admin.workspaces import workspaces


@click.group()
def admin() -> None:
    """
    Administer Paradime through the Administration API (Enterprise plan).

    Workspace-level commands take --workspace to target a workspace with a company-level key.
    """
    pass


admin.add_command(workspaces)
admin.add_command(git)
admin.add_command(connections)
admin.add_command(environments)
admin.add_command(env_vars)
admin.add_command(jobs)
admin.add_command(usage)
admin.add_command(setup_status)

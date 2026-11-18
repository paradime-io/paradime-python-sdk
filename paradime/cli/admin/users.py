from typing import Optional

import click

from paradime.apis.admin.users.types import UserAccountType
from paradime.cli import console
from paradime.cli.admin import common

ACCOUNT_TYPE_CHOICE = click.Choice(
    [account_type.value for account_type in UserAccountType if account_type.value != "UNKNOWN"],
    case_sensitive=False,
)


@click.group()
def users() -> None:
    """
    Invite, change and deactivate a workspace's users.
    """
    pass


@users.command()
@click.argument("email")
@click.option(
    "--account-type",
    type=ACCOUNT_TYPE_CHOICE,
    required=True,
    help="The role the invitation gives.",
)
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def create(email: str, account_type: str, workspace_uid: Optional[str], as_json: bool) -> None:
    """
    Invite EMAIL to the workspace and send them the invitation email.
    """
    user = common.get_admin().users.create(
        email, UserAccountType(account_type.upper()), workspace_uid=workspace_uid
    )
    common.print_model(user, as_json=as_json)


@users.command()
@click.argument("uid")
@click.option(
    "--account-type",
    type=ACCOUNT_TYPE_CHOICE,
    required=True,
    help="The role the user gets.",
)
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def update(uid: str, account_type: str, workspace_uid: Optional[str], as_json: bool) -> None:
    """
    Change the role of an active member.

    UID is the user's uid.
    """
    user = common.get_admin().users.update(
        uid, account_type=UserAccountType(account_type.upper()), workspace_uid=workspace_uid
    )
    common.print_model(user, as_json=as_json)


@users.command()
@click.argument("uid")
@click.confirmation_option(prompt="Deactivate this user?")
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def deactivate(uid: str, workspace_uid: Optional[str], as_json: bool) -> None:
    """
    Deactivate a member: they can no longer sign in to the workspace. Deactivating a former
    member again is not an error.

    UID is the user's uid.
    """
    user = common.get_admin().users.deactivate(uid, workspace_uid=workspace_uid)
    if as_json:
        common.print_model(user, as_json=True)
        return
    console.success(f"Deactivated user {user.email or user.uid} ({user.uid}).")

import io
import re
from typing import Dict, Optional, TextIO, Tuple

import click
from dotenv.parser import parse_stream

from paradime.cli import console
from paradime.cli.admin import common

NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
VALUES_ON_COMMAND_LINE = (
    "Values are never taken from the command line, where shell history and process lists "
    "keep them. Pipe the value on stdin or pass --file."
)

environment_option = click.option(
    "--environment",
    "environment_slug",
    default=None,
    help="An environment's slug, for its variables. Leave it out for the workspace's own.",
)


@click.group(name="env-vars")
def env_vars() -> None:
    """
    Manage environment variables of a workspace or one of its environments.

    Values are read only from stdin or a file, never from the command line.
    """
    pass


@env_vars.command(name="list")
@environment_option
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def list_variables(
    environment_slug: Optional[str], workspace_uid: Optional[str], as_json: bool
) -> None:
    """
    List the variables' names. Values are never shown.
    """
    common.print_models(
        common.get_admin().environment_variables.list(
            environment_slug=environment_slug, workspace_uid=workspace_uid
        ),
        columns=["Name", "Set", "Updated"],
        fields=["name", "is_set", "updated_at"],
        as_json=as_json,
    )


@env_vars.command(name="set")
@click.argument("name", required=False)
@click.option(
    "--file",
    "source",
    type=click.File("r"),
    default=None,
    help="Read from this file instead of stdin. With NAME, the file holds the value; without "
    "NAME, it holds NAME=VALUE lines (.env format).",
)
@click.option("--replace", is_flag=True, help="Delete the variables of the scope not given.")
@environment_option
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def set_variables(
    name: Optional[str],
    source: Optional[TextIO],
    replace: bool,
    environment_slug: Optional[str],
    workspace_uid: Optional[str],
    as_json: bool,
) -> None:
    """
    Set variables, from stdin or a file.

    \b
    With NAME, sets that variable to the whole input (one trailing newline dropped):
      printf '%s' "$TOKEN" | paradime admin env-vars set API_TOKEN
      paradime admin env-vars set API_TOKEN --file token.txt
    Without NAME, sets every NAME=VALUE line of the input (.env format):
      paradime admin env-vars set --file .env --environment production
    """
    if name is not None:
        _check_name(name)
    stream = source if source is not None else click.get_text_stream("stdin")
    if source is None and stream.isatty():
        raise click.UsageError(VALUES_ON_COMMAND_LINE)
    content = stream.read()
    variables = {name: _single_value(content)} if name is not None else _dotenv(content)
    if not variables:
        raise click.UsageError("The input holds no variables.")

    result = common.get_admin().environment_variables.set(
        variables,
        environment_slug=environment_slug,
        replace=replace,
        workspace_uid=workspace_uid,
    )
    if as_json:
        console.json_out(common.to_json(result))
        return
    changed = ", ".join(result.changed_names) if result.changed_names else "nothing"
    console.success(f"Changed {changed}. The scope holds {result.total_count} variables.")


def _check_name(name: str) -> None:
    if "=" in name:
        raise click.UsageError(VALUES_ON_COMMAND_LINE)
    if not NAME_PATTERN.match(name):
        raise click.UsageError(
            f"{name!r} is not a variable name: a letter or _, then letters, digits and _."
        )


def _single_value(content: str) -> str:
    for ending in ("\r\n", "\n"):
        if content.endswith(ending):
            return content[: -len(ending)]
    return content


def _dotenv(content: str) -> Dict[str, str]:
    variables: Dict[str, str] = {}
    missing = []
    for binding in parse_stream(io.StringIO(content)):
        if binding.error:
            raise click.UsageError(
                f"Line {binding.original.line} is not NAME=VALUE: "
                f"{binding.original.string.strip()!r}. Nothing was sent."
            )
        if binding.key is None:
            continue
        _check_name(binding.key)
        if binding.value is None:
            missing.append(binding.key)
        else:
            variables[binding.key] = binding.value
    if missing:
        raise click.UsageError(f"No value for {', '.join(missing)}: write NAME=VALUE.")
    return variables


@env_vars.command()
@click.argument("names", nargs=-1, required=True)
@environment_option
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def delete(
    names: Tuple[str, ...],
    environment_slug: Optional[str],
    workspace_uid: Optional[str],
    as_json: bool,
) -> None:
    """
    Delete variables. Names that don't exist are ignored.
    """
    deleted = common.get_admin().environment_variables.delete(
        list(names), environment_slug=environment_slug, workspace_uid=workspace_uid
    )
    if as_json:
        console.json_out({"deletedNames": deleted})
        return
    console.success(f"Deleted {', '.join(deleted) if deleted else 'nothing'}.")


@env_vars.command()
@click.argument("name")
@click.argument("new_name")
@environment_option
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def rename(
    name: str,
    new_name: str,
    environment_slug: Optional[str],
    workspace_uid: Optional[str],
    as_json: bool,
) -> None:
    """
    Rename a variable.
    """
    variable = common.get_admin().environment_variables.rename(
        name, new_name, environment_slug=environment_slug, workspace_uid=workspace_uid
    )
    if as_json:
        console.json_out(common.to_json(variable))
        return
    console.success(f"Renamed {name} to {variable.name}.")


@env_vars.command()
@click.argument("name")
@environment_option
@common.workspace_option
@common.handle_admin_errors
def reveal(name: str, environment_slug: Optional[str], workspace_uid: Optional[str]) -> None:
    """
    Print a variable's value. Needs the env_vars:reveal capability, and is audited.
    """
    value = common.get_admin().environment_variables.reveal(
        name, environment_slug=environment_slug, workspace_uid=workspace_uid
    )
    click.echo(value)

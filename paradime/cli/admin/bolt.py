from typing import Any, Dict, List, Optional, TextIO, Tuple

import click
from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError

from paradime.apis.admin.types import AdminInputModel
from paradime.apis.bolt.types import (
    BoltCommandConfigInput,
    BoltDeferredScheduleConfigInput,
    BoltEnvironmentVariableInput,
    BoltIntegrationsInput,
    BoltNotificationsInput,
    BoltScheduleTriggerInput,
    BoltSelfHealingConfigInput,
)
from paradime.cli import console
from paradime.cli.admin import common
from paradime.tools.pydantic import StrictStr, ValidationError

SCHEDULE_COLUMNS = ["Slug", "Name", "Schedule", "Environment", "Branch", "Suspended", "Updated"]
SCHEDULE_FIELDS = [
    "slug",
    "name",
    "schedule",
    "environment",
    "git_branch",
    "is_suspended",
    "updated_at",
]
REQUIRED_SETTINGS = ("name", "schedule", "environment")


class ScheduleSettings(AdminInputModel):
    name: Optional[StrictStr] = None
    schedule: Optional[StrictStr] = None
    environment: Optional[StrictStr] = None
    commands: Optional[List[StrictStr]] = None
    command_configs: Optional[List[BoltCommandConfigInput]] = None
    continue_on_error: Optional[bool] = None
    git_branch: Optional[StrictStr] = None
    description: Optional[StrictStr] = None
    timezone: Optional[StrictStr] = None
    owner_email: Optional[StrictStr] = None
    suspended: Optional[bool] = None
    sla_seconds: Optional[int] = None
    trigger_on_merge: Optional[bool] = None
    trigger_on_webhook: Optional[bool] = None
    notifications: Optional[BoltNotificationsInput] = None
    integrations: Optional[BoltIntegrationsInput] = None
    self_healing: Optional[BoltSelfHealingConfigInput] = None
    turbo_ci: Optional[BoltDeferredScheduleConfigInput] = None
    deferred_schedule: Optional[BoltDeferredScheduleConfigInput] = None
    schedule_trigger: Optional[BoltScheduleTriggerInput] = None
    env_vars: Optional[List[BoltEnvironmentVariableInput]] = None


def schedule_options(f: Any) -> Any:
    for option in reversed(
        [
            click.option(
                "--config",
                "config",
                type=click.File("r"),
                default=None,
                help="A YAML or JSON file with the schedule's settings, as the API takes them "
                "(name, schedule, environment, commands, notifications, integrations, ...). "
                "Use - for stdin. Options given on the command line win.",
            ),
            click.option("--name", default=None, help="The display name."),
            click.option(
                "--schedule", "cron", default=None, help="The cron expression, or OFF for none."
            ),
            click.option("--environment", default=None, help="The environment's slug."),
            click.option(
                "--command",
                "commands",
                multiple=True,
                help="A command each run runs; repeat it for several, in order.",
            ),
            click.option("--git-branch", default=None, help="The git branch runs use."),
            click.option("--timezone", default=None, help="The cron expression's timezone."),
            click.option("--owner-email", default=None, help="The owner's email."),
            click.option("--description", default=None, help="What the schedule is for."),
            click.option("--sla-seconds", type=int, default=None, help="How long a run may take."),
            click.option(
                "--trigger-on-merge", is_flag=True, help="Start a run on a merge to the branch."
            ),
            click.option(
                "--trigger-on-webhook", is_flag=True, help="Let Trigger URLs start the schedule."
            ),
            click.option(
                "--suspended/--active",
                "suspended",
                default=None,
                help="Pause or resume the schedule.",
            ),
        ]
    ):
        f = option(f)
    return f


def _settings(
    config: Optional[TextIO],
    *,
    name: Optional[str],
    cron: Optional[str],
    environment: Optional[str],
    commands: Tuple[str, ...],
    git_branch: Optional[str],
    timezone: Optional[str],
    owner_email: Optional[str],
    description: Optional[str],
    sla_seconds: Optional[int],
    trigger_on_merge: bool,
    trigger_on_webhook: bool,
    suspended: Optional[bool],
) -> Dict[str, Any]:
    settings = _config_settings(config) if config is not None else {}
    options = {
        "name": name,
        "schedule": cron,
        "environment": environment,
        "commands": list(commands) or None,
        "git_branch": git_branch,
        "timezone": timezone,
        "owner_email": owner_email,
        "description": description,
        "sla_seconds": sla_seconds,
        "trigger_on_merge": trigger_on_merge or None,
        "trigger_on_webhook": trigger_on_webhook or None,
        "suspended": suspended,
    }
    settings.update({key: value for key, value in options.items() if value is not None})
    missing = [setting for setting in REQUIRED_SETTINGS if settings.get(setting) is None]
    if missing:
        raise click.UsageError(
            f"Give {', '.join(missing)}: as --{', --'.join(missing)} or in --config."
        )
    return settings


def _config_settings(config: TextIO) -> Dict[str, Any]:
    try:
        data = YAML(typ="safe").load(config.read())
    except YAMLError as e:
        raise click.UsageError(f"--config is not valid YAML or JSON: {e}")
    if not isinstance(data, dict):
        raise click.UsageError("--config must hold the schedule's settings, such as `name: ...`.")
    try:
        settings = ScheduleSettings.parse_obj(data)
    except ValidationError as e:
        raise click.UsageError(f"--config has settings the API does not take:\n{e}")
    return {name: getattr(settings, name) for name in settings.__fields_set__}


@click.group()
def bolt() -> None:
    """
    Manage Bolt schedules created in the UI or through the API.
    """
    pass


@bolt.group()
def schedules() -> None:
    """
    Create, change, list and delete schedules created in the UI or through the API. YAML
    schedules are changed in the repository.
    """
    pass


@schedules.command(name="list")
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def list_schedules(workspace_uid: Optional[str], as_json: bool) -> None:
    """
    List the workspace's schedules created in the UI or through the API.
    """
    common.print_models(
        common.get_admin().bolt.list_schedules(workspace_uid=workspace_uid),
        columns=SCHEDULE_COLUMNS,
        fields=SCHEDULE_FIELDS,
        as_json=as_json,
    )


@schedules.command()
@click.option("--slug", default=None, help="The slug to give it; minted when left out.")
@schedule_options
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def create(
    slug: Optional[str],
    config: Optional[TextIO],
    workspace_uid: Optional[str],
    as_json: bool,
    **options: Any,
) -> None:
    """
    Create a schedule and deploy it. With --slug, running it again returns the same schedule.
    Give exactly one of --command or `command_configs` in --config.
    """
    schedule = common.get_admin().bolt.create_schedule(
        slug=slug, workspace_uid=workspace_uid, **_settings(config, **options)
    )
    common.print_model(schedule, as_json=as_json)


@schedules.command()
@click.argument("slug")
@schedule_options
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def update(
    slug: str,
    config: Optional[TextIO],
    workspace_uid: Optional[str],
    as_json: bool,
    **options: Any,
) -> None:
    """
    Replace a schedule's settings and redeploy it. Every setting is replaced, except that
    leaving out --suspended/--active, `continue_on_error` or `env_vars` keeps their values: give
    the whole schedule, with --config for notifications, integrations and the other nested
    settings. A YAML schedule is refused; change it in the repository.
    """
    schedule = common.get_admin().bolt.update_schedule(
        slug, workspace_uid=workspace_uid, **_settings(config, **options)
    )
    common.print_model(schedule, as_json=as_json)


@schedules.command()
@click.argument("slug")
@click.confirmation_option(prompt="Delete this schedule?")
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def delete(slug: str, workspace_uid: Optional[str], as_json: bool) -> None:
    """
    Delete a schedule and redeploy the schedules. Its slug is never given out again, and its
    Trigger URLs stay but start nothing. Deleting it again is not an error.
    """
    deleted = common.get_admin().bolt.delete_schedule(slug, workspace_uid=workspace_uid)
    if as_json:
        console.json_out({"deletedSlug": deleted})
        return
    console.success(f"Deleted schedule {deleted}.")

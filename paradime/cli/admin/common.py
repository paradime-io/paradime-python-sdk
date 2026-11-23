import functools
import json
import re
import sys
from typing import Any, Callable, Iterable, List, Optional, Sequence, TypeVar, cast

import click
from ruamel.yaml.constructor import DuplicateKeyError
from ruamel.yaml.error import YAMLError

from paradime.apis.admin.client import AdminClient
from paradime.apis.admin.exception import AdminJobTimeoutException
from paradime.apis.admin.jobs.types import AdminJob, DeployedBoltSchedulePage
from paradime.apis.admin.types import BoltScheduleLink, EnvironmentLink
from paradime.cli import console
from paradime.client.api_exception import ParadimeAPIException
from paradime.client.paradime_cli_client import get_cli_client_or_exit
from paradime.tools.pydantic import BaseModel

F = TypeVar("F", bound=Callable[..., Any])


def get_admin() -> AdminClient:
    return get_cli_client_or_exit().admin


def workspace_option(f: F) -> F:
    return click.option(
        "--workspace",
        "workspace_uid",
        default=None,
        help="The uid of the workspace to target, sent as X-Paradime-Workspace. Needed with a "
        "company API key when PARADIME_WORKSPACE_UID is not the workspace you mean.",
    )(f)


def json_option(f: F) -> F:
    return click.option("--json", "as_json", is_flag=True, help="JSON formatted response")(f)


def handle_admin_errors(f: F) -> F:
    @functools.wraps(f)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return f(*args, **kwargs)
        except (ParadimeAPIException, AdminJobTimeoutException) as e:
            console.error(str(e))
            sys.exit(1)

    return cast(F, wrapper)


def config_error(error: YAMLError) -> str:
    mark = getattr(error, "problem_mark", None)
    where = f" at line {mark.line + 1}, column {mark.column + 1}" if mark is not None else ""
    if isinstance(error, DuplicateKeyError):
        found = re.match(r'found duplicate key "([A-Za-z0-9_]+)"', str(error.problem or ""))
        key = f' "{found.group(1)}"' if found else ""
        return f"--config has a duplicate key{key}{where}."
    return f"--config is not valid YAML or JSON{where}."


def to_json(model: BaseModel) -> Any:
    return json.loads(model.json(by_alias=True))


def show(value: Optional[Any]) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, list):
        return ", ".join(str(item) for item in value)
    if isinstance(value, (EnvironmentLink, BoltScheduleLink)):
        return value.slug
    return str(getattr(value, "value", value))


def print_models(
    models: Iterable[BaseModel],
    *,
    columns: Sequence[str],
    fields: Sequence[str],
    as_json: bool,
    title: Optional[str] = None,
) -> None:
    rows: List[BaseModel] = list(models)
    if as_json:
        console.json_out([to_json(model) for model in rows])
        return
    console.table(
        columns,
        [[show(getattr(model, field)) for field in fields] for model in rows],
        title=title,
    )


def print_model(model: BaseModel, *, as_json: bool) -> None:
    if as_json:
        console.json_out(to_json(model))
        return
    for name, value in model:
        if isinstance(value, BaseModel):
            for child_name, child_value in value:
                console.kv(f"{name}.{child_name}", show(child_value))
        else:
            console.kv(name, show(value))


DEPLOYED_SCHEDULE_COLUMNS = ["Slug", "Source", "Failure reason"]
DEPLOYED_SCHEDULE_FIELDS = ["slug", "source", "failure_reason"]


def print_job_result(job: AdminJob) -> None:
    if job.result is None:
        return
    for name, value in job.result:
        if isinstance(value, DeployedBoltSchedulePage):
            console.kv(f"result.{name}", f"{len(value.nodes)} of {value.total_count}")
            print_models(
                value.nodes,
                columns=DEPLOYED_SCHEDULE_COLUMNS,
                fields=DEPLOYED_SCHEDULE_FIELDS,
                as_json=False,
            )
            if value.has_next_page:
                console.info(
                    f"Showing the first {len(value.nodes)} of {value.total_count} schedules. "
                    f"List them all with: paradime admin jobs deployed-schedules {job.id}"
                )
        else:
            console.kv(f"result.{name}", show(value))


def print_job(job: AdminJob, *, as_json: bool) -> None:
    if as_json:
        console.json_out(to_json(job))
        return
    for name, value in job:
        if name == "result":
            continue
        if isinstance(value, BaseModel):
            for child_name, child_value in value:
                console.kv(f"{name}.{child_name}", show(child_value))
        else:
            console.kv(name, show(value))
    print_job_result(job)


def wait_for_job(job: AdminJob, *, timeout: float, workspace_uid: Optional[str]) -> AdminJob:
    with console.spinner(f"Waiting for job {job.id}…"):
        return get_admin().jobs.wait(job.id, timeout=timeout, workspace_uid=workspace_uid)


def report_job(
    job: Optional[AdminJob],
    *,
    wait: bool,
    timeout: float,
    workspace_uid: Optional[str],
    as_json: bool,
) -> None:
    if job is None:
        return
    if wait:
        job = wait_for_job(job, timeout=timeout, workspace_uid=workspace_uid)
    if as_json:
        console.json_out({"job": to_json(job)})
        return
    console.kv("job", f"{job.id} ({job.status.value})")
    print_job_result(job)
    if not wait:
        console.info(f"Follow it with: paradime admin jobs wait {job.id}")


def wait_options(f: F) -> F:
    f = click.option(
        "--timeout",
        type=float,
        default=600,
        show_default=True,
        help="With --wait, how many seconds to wait.",
    )(f)
    return click.option("--wait", is_flag=True, help="Wait for the job to finish.")(f)

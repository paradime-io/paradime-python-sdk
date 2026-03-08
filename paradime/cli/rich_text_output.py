"""
paradime/cli/rich_text_output.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Backwards-compatible shim — delegates to paradime.cli.console.
New code should import from paradime.cli.console directly.
"""

from pathlib import Path

from paradime.cli import console as _c


def print_cli_header(version: str) -> None:
    _c.header(
        f"Paradime CLI v{version}",
        subtitle="Use the Paradime CLI to login and trigger Bolt dbt schedules from the terminal.",
    )
    _c.url(
        "Examples",
        "https://github.com/paradime-io/paradime-python-sdk/tree/main/examples",
    )


def print_error_table(error: str, is_json: bool) -> None:
    if is_json:
        _c.json_out({"error": error})
        return
    _c.error(error)


def print_run_started(run_id: int, is_json: bool) -> None:
    if is_json:
        _c.json_out({"run_id": run_id, "url": f"https://app.paradime.io/bolt/run_id/{run_id}"})
        return
    _c.success("Bolt run has started.")
    _c.url("Run details", f"https://app.paradime.io/bolt/run_id/{run_id}")


def print_success(message: str, is_json: bool) -> None:
    if is_json:
        _c.json_out({"message": message})
        return
    _c.success(message)


def print_run_status(status: str, json: bool) -> None:
    if not json:
        _c.info(f"Current run status: {status}")


def print_artifact_downloading(*, schedule_name: str, artifact_path: str) -> None:
    _c.info(f"Downloading {artifact_path!r} from schedule {schedule_name!r}…")


def print_artifact_downloaded(artifact_path: Path) -> None:
    _c.success(f"Artifact saved to {artifact_path.absolute().as_posix()!r}.")

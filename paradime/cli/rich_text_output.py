import json
from pathlib import Path

import click
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

console = Console()


def print_cli_header(version: str) -> None:
    version_title = Text("Paradime CLI ", style="green").append(f"v{version}", style="bold green")
    version_title.append(
        "\n\nUse the Paradime CLI to login and trigger Bolt dbt schedules from the terminal.",
        style="#9696a0",
    )
    version_title.append("\n\nRead examples of using the CLI and the SDK: ", style="#9696a0")
    version_title.append(
        "https://github.com/paradime-io/paradime-python-sdk/tree/main/examples",
        style="underline #9696a0",
    )
    console.print(Panel(version_title, padding=(1, 2), width=100), style="#827be6")


def print_error_table(error: str, is_json: bool) -> None:
    if is_json:
        click.echo(json.dumps({"error": error}))
        return

    table = Table(border_style="#787885", box=box.SIMPLE, show_footer=True, width=100)
    table.add_column("🚨 Error", justify="left", style="red", no_wrap=False)
    error_text = Text(error)
    table.add_row(error_text, style="#f44336")
    console.print(table)


def print_run_started(run_id: int, is_json: bool) -> None:
    if is_json:
        click.echo(
            json.dumps(
                {
                    "run_id": run_id,
                    "url": f"https://app.paradime.io/bolt/run_id/{run_id}",
                }
            )
        )
        return
    console.print(Text("\n🎉 Bolt run has started"))
    run_status_text = Text("\nCheck the run details at: \n", style="#787885")
    run_status_text.append(
        Text(f"https://app.paradime.io/bolt/run_id/{run_id}", style="underline #9696a0")
    )
    console.print(run_status_text)


def print_success(message: str, is_json: bool) -> None:
    if is_json:
        click.echo(json.dumps({"message": message}))
        return
    console.print(Text(f"🎉 {message}"), style="green")


def print_run_status(status: str, json: bool) -> None:
    if json:
        return
    console.print(Text(f"\n✨ Current run status: {status}"))


def print_artifact_downloading(*, schedule_name: str, artifact_path: str) -> None:
    console.print(
        Text(
            f"\n⬇️  Downloading the latest artifact located at {artifact_path!r} from schedule {schedule_name!r}..."
        )
    )


def print_artifact_downloaded(artifact_path: Path) -> None:
    console.print(Text(f"\n📦 Artifact downloaded to {artifact_path.absolute().as_posix()!r}."))

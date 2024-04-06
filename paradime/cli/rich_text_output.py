from typing import Any

from rich import box
from rich.console import Console
from rich.json import JSON
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

console = Console()


def print_cli_header(version: str) -> None:
    version_title = Text(f"Paradime CLI ", style="green").append(f"v{version}", style="bold green")
    version_title.append(
        f"\n\nUse the Paradime CLI to login and trigger Bolt dbt schedules from the terminal.",
        style="#9696a0",
    )
    version_title.append(f"\n\nRead examples of using the CLI and the SDK: ", style="#9696a0")
    version_title.append(
        f"https://github.com/paradime-io/paradime-python-sdk/tree/main/examples",
        style="underline #9696a0",
    )
    console.print(Panel(version_title, padding=(1, 2), width=100), style="#827be6")


def print_error_table(error: Any, json: bool) -> None:
    table = Table(border_style="#787885", box=box.SIMPLE, show_footer=True, width=100)
    table.add_column("🚨 Error", justify="left", style="red", no_wrap=False)
    if json:
        table.add_row(JSON.from_data(error, highlight=False), style="#f44336")
    else:
        table.add_row(Text(error), style="#f44336")
    console.print(table)


def print_run_started(run_id: int) -> None:
    console.print(Text("\n🎉 Bolt run has started"))
    run_status_text = Text("\nCheck the run details at: \n", style="#787885")
    run_status_text.append(
        Text(f"https://app.paradime.io/bolt/run_id/{run_id}", style="underline #9696a0")
    )
    console.print(run_status_text)


def print_run_status(status: str) -> None:
    console.print(Text(f"\n✨ Current run status: {status}"))

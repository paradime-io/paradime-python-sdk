import click

from paradime.cli import console
from paradime.version import get_sdk_version


@click.command()
def version() -> None:
    """
    Get the version of the Paradime CLI.
    """
    print_version()


def print_version() -> None:
    sdk_version = get_sdk_version()
    console.header(
        f"Paradime CLI v{sdk_version}",
        subtitle="Use the Paradime CLI to login and trigger Bolt dbt schedules from the terminal.",
    )
    console.url(
        "Examples",
        "https://github.com/paradime-io/paradime-python-sdk/tree/main/examples",
    )


if __name__ == "__main__":
    version()

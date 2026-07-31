"""Console-script entry point for the `paradime` command.

Deliberately free of third-party imports so that, when the CLI extra is not
installed, running `paradime` produces an actionable message instead of a
traceback about a missing click.
"""

import sys

_CLI_EXTRA_HINT = (
    "The `paradime` CLI needs extra dependencies that are not installed by default.\n"
    "Install them with:\n\n    pip install 'paradime-io[cli]'\n"
)


def main() -> None:
    try:
        from paradime.cli.cli import cli
    except ImportError as e:
        print(f"{_CLI_EXTRA_HINT}\nOriginal error: {e}", file=sys.stderr)
        sys.exit(1)

    cli()

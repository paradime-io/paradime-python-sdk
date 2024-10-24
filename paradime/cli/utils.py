import os
from typing import Callable

import click


def env_click_option(option_name: str, env_var: str, **kwargs) -> Callable:
    help = kwargs.get("help", "")
    if help:
        del kwargs["help"]
    return click.option(
        f"--{option_name}",
        prompt=False,
        default=None,
        envvar=env_var,
        required=True,
        help=f"{help}\n\n [env: {env_var}]",
        **kwargs,
    )

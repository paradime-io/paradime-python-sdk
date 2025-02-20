from typing import Any, Callable, Optional

import click


def env_click_option(option_name: str, env_var: Optional[str], **kwargs: Any) -> Callable:
    help = kwargs.pop("help", "")
    if env_var:
        help = f"{help}\n\n [env: {env_var}]"

    return click.option(
        f"--{option_name}",
        prompt=False,
        default=kwargs.pop("default", None),
        envvar=env_var,
        required=kwargs.pop("required", True),
        help=help,
        **kwargs,
    )

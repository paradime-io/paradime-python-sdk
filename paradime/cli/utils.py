from typing import Any, Callable, List, Optional

import click


class CommaSeparatedList(click.ParamType):
    """A Click parameter type that accepts comma-separated values.

    Usage: --connector-ids id1,id2,id3
    """

    name = "COMMA_LIST"

    def convert(
        self, value: Any, param: Optional[click.Parameter], ctx: Optional[click.Context]
    ) -> List[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return value
        return [item.strip() for item in str(value).split(",") if item.strip()]


COMMA_LIST = CommaSeparatedList()


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

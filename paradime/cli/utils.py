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


def deprecated_alias_option(old_name: str, new_name: str, **kwargs: Any) -> Callable:
    """Add a hidden deprecated alias for a renamed CLI option.

    The old option name is added as a hidden click option. The value is
    stored under a parameter name derived from old_name (with dashes replaced
    by underscores).
    """
    return click.option(
        f"--{old_name}",
        hidden=True,
        expose_value=True,
        **kwargs,
    )


def resolve_deprecated_option(
    new_value: Any,
    old_value: Any,
    new_flag: str,
    old_flag: str,
) -> Any:
    """Merge a new option value with its deprecated alias.

    If both are provided, raise a UsageError. If only the old one is provided,
    return it. Otherwise return the new value.
    """
    old_set = _is_set(old_value)
    new_set = _is_set(new_value)

    if old_set and new_set:
        raise click.UsageError(
            f"Cannot specify both --{old_flag} and --{new_flag}. Use --{new_flag}."
        )
    if old_set:
        return old_value
    return new_value


def _is_set(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, (list, tuple)) and len(value) == 0:
        return False
    return True

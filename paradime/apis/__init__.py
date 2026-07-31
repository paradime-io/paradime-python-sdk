from typing import TYPE_CHECKING, Any, List

from .bolt import BoltClient

if TYPE_CHECKING:
    from .metadata import MetadataClient

__all__ = [
    "BoltClient",
    "MetadataClient",
]


def __getattr__(name: str) -> Any:
    """Import MetadataClient on first access rather than at package import.

    The metadata client pulls in duckdb, polars, pyarrow and dbt-artifacts-parser.
    Importing it eagerly meant `from paradime import Paradime` loaded all of them,
    so they had to be installed even for callers who only trigger Bolt runs. They
    now live behind the `metadata` extra.
    """

    if name == "MetadataClient":
        from .metadata import MetadataClient

        return MetadataClient
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> List[str]:
    return sorted(__all__)

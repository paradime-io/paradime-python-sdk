"""Loader for the SDK's GraphQL operation documents.

Operations live as ``.graphql`` files under ``paradime/graphql/<domain>/`` rather
than as string literals in the client methods, so they can be read, diffed and
linted as GraphQL.

Usage::

    from paradime.graphql import load_operation

    query = load_operation("bolt", "listBoltSchedules")
"""

from functools import lru_cache
from importlib import resources

__all__ = ["load_operation"]


@lru_cache(maxsize=None)
def load_operation(domain: str, name: str) -> str:
    """Return the text of ``paradime/graphql/<domain>/<name>.graphql``.

    Results are cached, so repeated calls do not re-read from disk (or from the
    zip, for a zipped install).

    Args:
        domain: The sub-package directory, e.g. ``"bolt"``.
        name: The operation name, without the ``.graphql`` suffix.

    Raises:
        FileNotFoundError: If no such operation file is packaged.
    """

    package = f"{__name__}.{domain}"
    try:
        return resources.files(package).joinpath(f"{name}.graphql").read_text(encoding="utf-8")
    except (FileNotFoundError, ModuleNotFoundError) as e:
        raise FileNotFoundError(
            f"No GraphQL operation {name!r} in domain {domain!r}. Expected to find "
            f"paradime/graphql/{domain}/{name}.graphql in the installed package."
        ) from e

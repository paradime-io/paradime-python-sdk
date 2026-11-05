import json
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, Generic, Iterator, List, Optional, TypeVar, Union

from paradime.tools.pydantic import BaseModel

T = TypeVar("T")

MAX_PAGE_SIZE = 500
DEFAULT_PAGE_SIZE = 100


ALIASES = {
    "schema_name": "schema",
    "validate_repository": "validate",
    "period_from": "from",
    "period_to": "to",
}

SECRET_FIELDS = frozenset(
    {
        "value",
        "password",
        "private_key",
        "private_key_passphrase",
        "client_secret",
        "service_account_json",
        "token",
        "profile",
    }
)


def _to_camel(name: str) -> str:
    if name in ALIASES:
        return ALIASES[name]
    first, *rest = name.split("_")
    return first + "".join(word.capitalize() for word in rest)


class AdminModel(BaseModel):
    class Config:
        alias_generator = _to_camel
        allow_population_by_field_name = True


class AdminInputModel(AdminModel):
    class Config:
        extra = "forbid"

    def __repr_args__(self) -> Any:
        return [
            (name, "**********" if name in SECRET_FIELDS and value is not None else value)
            for name, value in super().__repr_args__()
        ]


class OpenEnum(str, Enum):
    @classmethod
    def _missing_(cls, value: object) -> Any:
        return cls.__members__.get("UNKNOWN")


class _Unset:
    def __repr__(self) -> str:
        return "UNSET"

    def __bool__(self) -> bool:
        return False


UNSET: Any = _Unset()

Settable = Union[T, None, _Unset]


def given(**values: Any) -> Dict[str, Any]:
    return {name: value for name, value in values.items() if value is not UNSET}


def serialize_input(model: BaseModel) -> Dict[str, Any]:
    return json.loads(model.json(by_alias=True, exclude_unset=True))


@dataclass
class Page(Generic[T]):
    nodes: List[T]
    total_count: int
    end_cursor: Optional[str]
    has_next_page: bool


CONNECTION_FIELDS = """
    totalCount
    pageInfo {
        endCursor
        hasNextPage
    }
"""


def parse_page(connection: Optional[Dict[str, Any]], parse_node: Callable[[Any], T]) -> Page[T]:
    if connection is None:
        return Page(nodes=[], total_count=0, end_cursor=None, has_next_page=False)
    page_info = connection.get("pageInfo") or {}
    return Page(
        nodes=[parse_node(edge["node"]) for edge in connection.get("edges") or []],
        total_count=connection.get("totalCount") or 0,
        end_cursor=page_info.get("endCursor"),
        has_next_page=bool(page_info.get("hasNextPage")),
    )


def check_page_size(page_size: int) -> int:
    if not 1 <= page_size <= MAX_PAGE_SIZE:
        raise ValueError(f"page_size must be between 1 and {MAX_PAGE_SIZE}, got {page_size}.")
    return page_size


def iterate_pages(fetch_page: Callable[[Optional[str]], Page[T]]) -> Iterator[T]:
    after: Optional[str] = None
    while True:
        page = fetch_page(after)
        yield from page.nodes
        if not page.has_next_page or not page.end_cursor or page.end_cursor == after:
            return
        after = page.end_cursor

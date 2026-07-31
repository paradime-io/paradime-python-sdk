"""Shared pydantic bases for the SDK's request and response models.

The Paradime API speaks camelCase; the SDK exposes snake_case. Rather than
mapping the two by hand at every call site, both bases carry an alias generator
so pydantic does the translation.
"""

from paradime.tools.pydantic import BaseModel, Extra


def snake_to_camel(snake: str) -> str:
    """Convert a snake_case field name to the camelCase name used on the wire."""

    head, *tail = snake.split("_")
    return head + "".join(part.title() for part in tail)


class ParadimeInputModel(BaseModel):
    """Base for models the caller constructs and the SDK sends to the API.

    Serialize with ``.dict(by_alias=True, exclude_none=True)`` to get the
    GraphQL payload. ``extra = forbid`` is right here: a typo in a field name
    the caller typed should be an error, not a silently dropped value.
    """

    class Config:
        alias_generator = snake_to_camel
        allow_population_by_field_name = True
        extra = Extra.forbid


class ParadimeResponseModel(BaseModel):
    """Base for models parsed from an API response.

    Parse with ``parse_obj_as(Model, response_json)`` — the alias generator maps
    the camelCase wire fields onto the snake_case attributes.

    ``extra = ignore`` (pydantic's default, stated explicitly because it matters)
    so that a field added server-side does not break existing SDK versions.
    """

    class Config:
        alias_generator = snake_to_camel
        allow_population_by_field_name = True
        extra = Extra.ignore

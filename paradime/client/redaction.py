import re
from typing import Callable, TypeVar
from urllib.parse import urlsplit

import requests

COMPANY_TOKEN_PATH = re.compile(r"(/api/v\d+/)([^/?#]+)")
QUERY_STRING = re.compile(r"\?[^\s'\"<>()]+")

T = TypeVar("T")


def redact_endpoint(endpoint: str) -> str:
    return COMPANY_TOKEN_PATH.sub(r"\1***", endpoint)


def redact_company_token(text: str, endpoint: str) -> str:
    redacted = COMPANY_TOKEN_PATH.sub(r"\1***", text)
    match = COMPANY_TOKEN_PATH.search(endpoint)
    if match:
        redacted = redacted.replace(match.group(2), "***")
    return redacted


def redact_query_strings(text: str, url: str) -> str:
    query = urlsplit(url).query
    if query:
        text = text.replace(f"?{query}", "").replace(query, "")
    return QUERY_STRING.sub("", text)


def same_type_error(error: requests.RequestException, message: str) -> requests.RequestException:
    try:
        return type(error)(message)
    except Exception:
        return requests.RequestException(message)


def redacted_request_error(
    error: requests.RequestException, endpoint: str
) -> requests.RequestException:
    return same_type_error(error, redact_company_token(str(error), endpoint))


def send_redacted(
    send: Callable[[], T], redact: Callable[[requests.RequestException], Exception]
) -> T:
    try:
        return send()
    except requests.RequestException as e:
        error = redact(e)
    raise error


def download(url: str) -> requests.Response:
    return send_redacted(
        lambda: requests.get(url),
        lambda e: same_type_error(e, redact_query_strings(str(e), url)),
    )

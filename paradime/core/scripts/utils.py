from typing import Optional

from requests import Response


class HTTPRequestException(Exception):
    pass


def handle_http_error(response: Response, prepend_error_msg: Optional[str] = "") -> None:
    spaced_prepend_error_msg = f"{prepend_error_msg} " if prepend_error_msg else ""
    try:
        response.raise_for_status()
    except Exception as e:
        raise HTTPRequestException(spaced_prepend_error_msg + response.text) from e

from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Type

from paradime.client.api_exception import ParadimeAPIException, ParadimeException


class ErrorCode(str, Enum):
    UNAUTHENTICATED = "UNAUTHENTICATED"
    MISSING_CAPABILITY = "MISSING_CAPABILITY"
    WORKSPACE_REQUIRED = "WORKSPACE_REQUIRED"
    NOT_FOUND = "NOT_FOUND"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    CONNECTION_TEST_FAILED = "CONNECTION_TEST_FAILED"
    CONFLICT = "CONFLICT"
    RATE_LIMITED = "RATE_LIMITED"
    PLAN_UPGRADE_REQUIRED = "PLAN_UPGRADE_REQUIRED"
    INTERNAL = "INTERNAL"


class AdminApiException(ParadimeAPIException):
    """
    An Admin API error, as the server's `extensions.code` names it.

    Attributes:
        message (str): What went wrong, in words.
        code (ErrorCode, optional): The error's code; None when the server sent no known code.
        field (List[str], optional): The input path the error is about, such as
            ``["input", "variables", "0", "value"]``.
        extensions (dict): The error's `extensions`, as the server sent them.
        errors (List[dict]): Every error of the response; this exception describes the first.
        job (AdminJob, optional): The failed job, when the error is a job's failure.
    """

    error_code: Optional[ErrorCode] = None

    def __init__(
        self,
        message: str,
        *,
        code: Optional[ErrorCode] = None,
        field: Optional[List[str]] = None,
        extensions: Optional[Dict[str, Any]] = None,
        errors: Optional[List[Dict[str, Any]]] = None,
    ):
        super().__init__(message)
        self.message = message
        self.code = code if code is not None else self.error_code
        self.field = field
        self.extensions = extensions or {}
        self.errors = errors or []
        self.job: Any = None

    def __reduce__(self) -> Any:
        return (_restore_exception, (type(self), self.args, self.__dict__))

    def __str__(self) -> str:
        prefix = f"{self.code.value}: " if self.code else ""
        suffix = f" (field: {'.'.join(self.field)})" if self.field else ""
        return f"{prefix}{self.message}{suffix}"


class AdminUnauthenticatedException(AdminApiException):
    error_code = ErrorCode.UNAUTHENTICATED


class AdminMissingCapabilityException(AdminApiException):
    error_code = ErrorCode.MISSING_CAPABILITY


class AdminWorkspaceRequiredException(AdminApiException):
    error_code = ErrorCode.WORKSPACE_REQUIRED


class AdminNotFoundException(AdminApiException):
    error_code = ErrorCode.NOT_FOUND


class AdminValidationFailedException(AdminApiException):
    error_code = ErrorCode.VALIDATION_FAILED


class AdminConnectionTestFailedException(AdminApiException):
    error_code = ErrorCode.CONNECTION_TEST_FAILED


class AdminConflictException(AdminApiException):
    error_code = ErrorCode.CONFLICT


class AdminRateLimitedException(AdminApiException):
    """
    The API key used up its rate limit. ``retry_after`` is how many seconds to wait, when the
    server said.
    """

    error_code = ErrorCode.RATE_LIMITED

    def __init__(self, message: str, *, retry_after: Optional[float] = None, **kwargs: Any):
        super().__init__(message, **kwargs)
        self.retry_after = retry_after


class AdminPlanUpgradeRequiredException(AdminApiException):
    error_code = ErrorCode.PLAN_UPGRADE_REQUIRED


class AdminInternalException(AdminApiException):
    error_code = ErrorCode.INTERNAL


class AdminJobTimeoutException(ParadimeException, TimeoutError):
    """
    An Admin job did not finish in time. ``job`` is its last state.
    """

    def __init__(self, message: str, *, job: Any):
        super().__init__(message)
        self.job = job

    def __reduce__(self) -> Any:
        return (_restore_exception, (type(self), self.args, self.__dict__))


def _restore_exception(
    exception_class: Type[Exception], args: Tuple[Any, ...], state: Dict[str, Any]
) -> Exception:
    exception = exception_class.__new__(exception_class)
    Exception.__init__(exception, *args)
    exception.__dict__.update(state)
    return exception


EXCEPTIONS_BY_CODE: Dict[ErrorCode, Type[AdminApiException]] = {
    exception.error_code: exception
    for exception in (
        AdminUnauthenticatedException,
        AdminMissingCapabilityException,
        AdminWorkspaceRequiredException,
        AdminNotFoundException,
        AdminValidationFailedException,
        AdminConnectionTestFailedException,
        AdminConflictException,
        AdminRateLimitedException,
        AdminPlanUpgradeRequiredException,
        AdminInternalException,
    )
    if exception.error_code is not None
}


def parse_error_code(value: Any) -> Optional[ErrorCode]:
    try:
        return ErrorCode(value)
    except ValueError:
        return None


def parse_retry_after(value: Any) -> Optional[float]:
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return None
    return seconds if seconds >= 0 else None


def admin_exception_for(
    code: Optional[ErrorCode],
    message: str,
    *,
    field: Optional[List[str]] = None,
    extensions: Optional[Dict[str, Any]] = None,
    errors: Optional[List[Dict[str, Any]]] = None,
    retry_after: Optional[float] = None,
) -> AdminApiException:
    exception_class = EXCEPTIONS_BY_CODE.get(code, AdminApiException) if code else AdminApiException
    if exception_class is AdminRateLimitedException:
        return AdminRateLimitedException(
            message,
            retry_after=retry_after,
            field=field,
            extensions=extensions,
            errors=errors,
        )
    return exception_class(message, code=code, field=field, extensions=extensions, errors=errors)


def admin_exception_from_errors(
    errors: List[Dict[str, Any]], *, retry_after_header: Optional[str] = None
) -> AdminApiException:
    first = errors[0] if errors and isinstance(errors[0], dict) else {}
    extensions = first.get("extensions") or {}
    if not isinstance(extensions, dict):
        extensions = {}
    field = extensions.get("field")
    if isinstance(field, str):
        field = [field]
    elif isinstance(field, list):
        field = [str(part) for part in field]
    else:
        field = None
    retry_after = parse_retry_after(extensions.get("retryAfter"))
    if retry_after is None:
        retry_after = parse_retry_after(retry_after_header)
    return admin_exception_for(
        parse_error_code(extensions.get("code")),
        str(first.get("message") or errors),
        field=field,
        extensions=extensions,
        errors=errors,
        retry_after=retry_after,
    )

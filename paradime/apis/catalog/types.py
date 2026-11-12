from enum import Enum
from typing import List, Optional

from paradime.tools.pydantic import BaseModel


class CatalogRefreshStatus(str, Enum):
    """The status of a catalog refresh as a whole."""

    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    LOST = "LOST"
    SKIPPED = "SKIPPED"


class CatalogRefreshStepStatus(str, Enum):
    """The status of one step of a catalog refresh.

    LOST means the step was running but stopped reporting, so it is treated as failed.
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    LOST = "LOST"


class CatalogRefreshError(BaseModel):
    code: str
    message: str
    detail: Optional[str] = None
    """The raw cause, e.g. the end of the dbt output for a failed docs generate."""


class CatalogRefreshProgress(BaseModel):
    """Measured progress of a running step."""

    stage: str
    stage_label: str
    done: int
    total: int
    """0 when the stage cannot measure its total."""
    unit: str


class CatalogRefreshStep(BaseModel):
    kind: str
    """What the step does, e.g. `docs_generate.dbt`, `catalog.build` or `datahub.push`."""
    label: Optional[str] = None
    """The step as a person reads it, e.g. `Catalog build`."""
    status: CatalogRefreshStepStatus
    blocking: bool
    """False for steps whose failure does not fail the refresh, e.g. an integration push."""
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    progress: Optional[CatalogRefreshProgress] = None
    error: Optional[CatalogRefreshError] = None

    @property
    def is_done(self) -> bool:
        return self.status not in (
            CatalogRefreshStepStatus.PENDING,
            CatalogRefreshStepStatus.RUNNING,
        )

    @property
    def is_failed(self) -> bool:
        return self.status in (CatalogRefreshStepStatus.FAILED, CatalogRefreshStepStatus.LOST)


class CatalogRefresh(BaseModel):
    """A catalog refresh and the state of each of its steps."""

    token: str
    status: CatalogRefreshStatus
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    error: Optional[CatalogRefreshError] = None
    finished: bool = False
    """The status is final: nothing in the refresh is still going."""
    steps: List[CatalogRefreshStep]


class CatalogRefreshRequest(BaseModel):
    """The result of requesting a catalog refresh."""

    token: Optional[str] = None
    """The id of the refresh, to follow it with `CatalogClient.get_refresh`."""
    joined: bool = False
    """True when a refresh was already running and is returned instead of a new one."""

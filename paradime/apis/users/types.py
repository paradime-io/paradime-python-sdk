from enum import Enum
from typing import List, Optional

from paradime.tools.pydantic import BaseModel


class UserAccountType(str, Enum):
    ADMIN = "ADMIN"
    DEVELOPER = "DEVELOPER"
    BUSINESS = "BUSINESS"
    BILLING_ADMIN = "BILLING_ADMIN"
    SECURITY_ADMIN = "SECURITY_ADMIN"
    WORKSPACE_SETTINGS_ADMIN = "WORKSPACE_SETTINGS_ADMIN"
    WORKSPACE_ADMIN = "WORKSPACE_ADMIN"
    ANALYST = "ANALYST"
    SANDBOX_USER = "SANDBOX_USER"


class InviteStatus(str, Enum):
    SENT = "SENT"
    EXPIRED = "EXPIRED"


class ActiveUser(BaseModel):
    uid: str
    email: str
    name: str
    account_type: str


class InvitedUser(BaseModel):
    email: str
    account_type: str
    invite_status: str


class SeatProduct(str, Enum):
    CODE_IDE = "CODE_IDE"
    BOLT = "BOLT"


class SeatHolder(BaseModel):
    uid: str
    name: str
    email: Optional[str]
    assigned_at: Optional[str]


class SeatPool(BaseModel):
    """Counts are company-wide; ``holders`` are the users of the key's workspace."""

    product: SeatProduct
    seats: Optional[int]
    scheduled_seats: Optional[int]
    scheduled_from: Optional[str]
    paid_seats: Optional[int]
    contracted_seats: Optional[int]
    assigned: Optional[int]
    available: Optional[int]
    holders: List[SeatHolder]


class SeatAssignmentPreview(BaseModel):
    """What ``assign_seat`` would do. With ``needs_purchase``, assign with
    ``confirm_purchase=True`` and ``expected_seats_after=seats_after`` to buy the seat."""

    pool: SeatPool
    already_assigned: Optional[bool]
    needs_purchase: Optional[bool]
    seats_after: Optional[int]
    proration_cents: Optional[int]


class SeatAssignment(BaseModel):
    """The pool after the assignment. A full pool without a confirmed purchase raises
    ``ParadimeAPIException`` whose message starts with ``PURCHASE_CONFIRMATION_REQUIRED``."""

    pool: SeatPool
    holder: Optional[SeatHolder]
    assigned: Optional[bool]
    purchased: Optional[bool]

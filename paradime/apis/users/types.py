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
    uid: Optional[str]
    name: Optional[str]
    email: Optional[str]
    assigned_at: str


class SeatPool(BaseModel):
    product: SeatProduct
    seats: Optional[int]
    scheduled_seats: Optional[int]
    scheduled_from: Optional[str]
    paid_seats: Optional[int]
    contracted_seats: Optional[int]
    assigned: int
    available: Optional[int]
    holders: List[SeatHolder]


class SeatAssignment(BaseModel):
    """``ok`` is False with ``needs_purchase`` when the pool is full: assign again with
    ``confirm_purchase=True`` and ``expected_seats_after=seats_after`` to buy the seat."""

    ok: bool
    needs_purchase: Optional[bool]
    assigned: Optional[bool]
    purchased: Optional[bool]
    seats: Optional[int]
    seats_after: Optional[int]
    proration_cents: Optional[int]

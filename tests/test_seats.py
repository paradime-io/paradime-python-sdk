from typing import Any, Dict, List, Optional

from paradime.apis.users.client import UsersClient
from paradime.apis.users.types import SeatProduct

POOL = {
    "product": "CODE_IDE",
    "seats": 3,
    "scheduledSeats": None,
    "scheduledFrom": None,
    "paidSeats": 3,
    "contractedSeats": 0,
    "assigned": 1,
    "available": 2,
}


class FakeAPIClient:
    def __init__(self, *responses: Dict[str, Any]) -> None:
        self.responses = list(responses)
        self.calls: List[Dict[str, Any]] = []

    def _call_gql(self, *, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        self.calls.append(variables)
        return self.responses.pop(0)


def _users(*responses: Dict[str, Any]) -> UsersClient:
    return UsersClient(FakeAPIClient(*responses))  # type: ignore[arg-type]


def _page(uids: List[str], end: Optional[str]) -> Dict[str, Any]:
    return {
        "seatPool": {
            **POOL,
            "holders": {
                "pageInfo": {"hasNextPage": end is not None, "endCursor": end},
                "edges": [
                    {"node": {"uid": uid, "name": uid, "email": None, "assignedAt": None}}
                    for uid in uids
                ],
            },
        }
    }


def test_list_seats_reads_every_page_of_holders() -> None:
    users = _users(_page(["u1", "u2"], "cursor-2"), _page(["u3"], None))

    pool = users.list_seats(SeatProduct.CODE_IDE)

    assert [holder.uid for holder in pool.holders] == ["u1", "u2", "u3"]
    assert (pool.seats, pool.available) == (3, 2)
    calls = users.client.calls  # type: ignore[attr-defined]
    assert [call["after"] for call in calls] == [None, "cursor-2"]


def test_preview_seat_assignment() -> None:
    users = _users(
        {
            "seatAssignmentPreview": {
                "alreadyAssigned": False,
                "needsPurchase": True,
                "seatsAfter": 4,
                "prorationCents": 2200,
                "seatPool": POOL,
            }
        }
    )

    preview = users.preview_seat_assignment("u1", SeatProduct.BOLT)

    assert (preview.needs_purchase, preview.seats_after, preview.proration_cents) == (
        True,
        4,
        2200,
    )
    assert users.client.calls == [{"uid": "u1", "product": "BOLT"}]  # type: ignore[attr-defined]


def test_assign_seat_sends_one_input() -> None:
    users = _users(
        {
            "assignSeat": {
                "assigned": True,
                "purchased": True,
                "holder": {"uid": "u1", "name": "Anna", "email": None, "assignedAt": None},
                "seatPool": {**POOL, "seats": 4},
            }
        }
    )

    result = users.assign_seat(
        "u1", SeatProduct.CODE_IDE, confirm_purchase=True, expected_seats_after=4
    )

    assert (result.assigned, result.purchased, result.pool.seats) == (True, True, 4)
    assert result.holder is not None and result.holder.uid == "u1"
    assert users.client.calls == [  # type: ignore[attr-defined]
        {
            "input": {
                "uid": "u1",
                "product": "CODE_IDE",
                "confirmPurchase": True,
                "expectedSeatsAfter": 4,
            }
        }
    ]


def test_unassign_seat() -> None:
    users = _users({"unassignSeat": {"freed": True}})

    assert users.unassign_seat("u1", SeatProduct.BOLT) is True
    assert users.client.calls == [  # type: ignore[attr-defined]
        {"input": {"uid": "u1", "product": "BOLT"}}
    ]

from typing import Any, Dict, Optional

from paradime.apis.users.client import UsersClient
from paradime.apis.users.types import SeatProduct


class FakeAPIClient:
    def __init__(self, response: Dict[str, Any]) -> None:
        self.response = response
        self.variables: Optional[Dict[str, Any]] = None

    def _call_gql(self, *, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        self.variables = variables
        return self.response


def _users(response: Dict[str, Any]) -> UsersClient:
    return UsersClient(FakeAPIClient(response))  # type: ignore[arg-type]


def test_list_seats() -> None:
    users = _users(
        {
            "listSeats": {
                "product": "CODE_IDE",
                "seats": 3,
                "scheduledSeats": None,
                "scheduledFrom": None,
                "paidSeats": 3,
                "contractedSeats": 0,
                "assigned": 1,
                "available": 2,
                "holders": [
                    {
                        "uid": "u1",
                        "name": "Anna",
                        "email": "anna@co.com",
                        "assignedAt": "2026-11-01T00:00:00+00:00",
                    }
                ],
            }
        }
    )

    pool = users.list_seats(SeatProduct.CODE_IDE)

    assert pool.available == 2
    assert pool.holders[0].uid == "u1"
    assert users.client.variables == {"product": "CODE_IDE"}  # type: ignore[attr-defined]


def test_assign_seat_reports_a_purchase_to_confirm() -> None:
    users = _users(
        {
            "assignSeat": {
                "ok": False,
                "needsPurchase": True,
                "assigned": None,
                "purchased": None,
                "seats": 3,
                "seatsAfter": 4,
                "prorationCents": 2200,
            }
        }
    )

    result = users.assign_seat("u1", SeatProduct.BOLT)

    assert not result.ok and result.needs_purchase and result.seats_after == 4
    assert users.client.variables == {  # type: ignore[attr-defined]
        "uid": "u1",
        "product": "BOLT",
        "confirmPurchase": False,
        "expectedSeatsAfter": None,
    }


def test_assign_seat_confirms_the_figure() -> None:
    users = _users(
        {
            "assignSeat": {
                "ok": True,
                "needsPurchase": False,
                "assigned": True,
                "purchased": True,
                "seats": 4,
                "seatsAfter": None,
                "prorationCents": None,
            }
        }
    )

    result = users.assign_seat(
        "u1", SeatProduct.CODE_IDE, confirm_purchase=True, expected_seats_after=4
    )

    assert result.ok and result.purchased
    assert users.client.variables["expectedSeatsAfter"] == 4  # type: ignore[attr-defined,index]


def test_unassign_seat() -> None:
    users = _users({"unassignSeat": {"freed": True}})

    assert users.unassign_seat("u1", SeatProduct.CODE_IDE) is True

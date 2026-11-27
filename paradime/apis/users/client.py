from typing import Any, Dict, List, Optional

from paradime.apis.users.types import (
    ActiveUser,
    InvitedUser,
    SeatAssignment,
    SeatAssignmentPreview,
    SeatHolder,
    SeatPool,
    SeatProduct,
    UserAccountType,
)
from paradime.client.api_client import APIClient


class UsersClient:
    def __init__(self, client: APIClient):
        self.client = client

    def list_active(self) -> List[ActiveUser]:
        """
        Retrieves all active users.

        Returns:
            List[ActiveUser]: A list of active user objects.
        """

        query = """
            query listActiveUsers {
                listUsers{
                    activeUsers{
                        uid
                        email
                        name
                        accountType
                    }
                }
            }
        """

        response = self.client._call_gql(query)
        return [
            ActiveUser(
                uid=user["uid"],
                email=user["email"],
                name=user["name"],
                account_type=user["accountType"],
            )
            for user in response["listUsers"]["activeUsers"]
        ]

    def get_by_email(self, email: str) -> ActiveUser:
        """
        Retrieves a user by email.

        Args:
            email (str): The email of the user to retrieve.

        Returns:
            ActiveUser: The user object.
        """

        active_users = self.list_active()
        for user in active_users:
            if user.email == email:
                return user

        raise ValueError(f"No active user found with email {email!r}")

    def list_invited(self) -> List[InvitedUser]:
        """
        Retrieves all invited users.

        Returns:
            List[InvitedUser]: A list of invited user objects.
        """

        query = """
            query listInvitedUsers {
                listUsers{
                    invitedUsers{
                        email
                        accountType
                        inviteStatus
                    }
                }
            }
        """

        response = self.client._call_gql(query)
        return [
            InvitedUser(
                email=user["email"],
                account_type=user["accountType"],
                invite_status=user["inviteStatus"],
            )
            for user in response["listUsers"]["invitedUsers"]
        ]

    def invite(self, email: str, account_type: UserAccountType) -> None:
        """
        Invites a user to the workspace.

        Args:
            email (str): The email of the user to invite.
            account_type (UserAccountType): The account type of the user to invite.
        """

        query = """
            mutation inviteUser($email: String!, $accountType: UserAccountType!) {
                inviteUser(email: $email, accountType: $accountType){
                    ok
                }
            }
        """

        self.client._call_gql(
            query=query,
            variables={"email": email, "accountType": account_type.value},
        )

    def update_account_type(self, user_uid: str, account_type: UserAccountType) -> None:
        """
        Updates the account type of a user.

        Args:
            uid (str): The ID of the user to update the account type for.
            account_type (UserAccountType): The new account type for the user.
        """

        query = """
            mutation updateUserAccountType($uid: String!, $accountType: UserAccountType!) {
                updateUserAccountType(uid: $uid, accountType: $accountType){
                    ok
                }
            }
        """

        self.client._call_gql(
            query=query,
            variables={"uid": user_uid, "accountType": account_type.value},
        )

    def disable(self, user_uid: str) -> None:
        query = """
                mutation disableUser($uid: String!) {
                    disableUser(uid: $uid){
                        ok
                    }
                }
            """

        self.client._call_gql(query=query, variables={"uid": user_uid})

    def list_seats(self, product: SeatProduct) -> SeatPool:
        """
        Retrieves the company's seat pool for a product, and which users of the
        workspace hold a seat. Needs the users:admin:read permission.

        Args:
            product (SeatProduct): The seat product.

        Returns:
            SeatPool: The pool and every holder in the workspace.
        """

        query = (
            """
            query seatPool($product: SeatProduct!, $after: String) {
                seatPool(product: $product) {
                    """
            + _POOL_FIELDS
            + """
                    holders(first: 500, after: $after) {
                        pageInfo { hasNextPage endCursor }
                        edges { node { uid name email assignedAt } }
                    }
                }
            }
        """
        )

        holders: List[SeatHolder] = []
        after: Optional[str] = None
        while True:
            pool = self.client._call_gql(
                query=query, variables={"product": product.value, "after": after}
            )["seatPool"]
            page = pool["holders"]
            holders.extend(_holder(edge["node"]) for edge in page["edges"])
            if not page["pageInfo"]["hasNextPage"]:
                return _pool(pool, holders)
            after = page["pageInfo"]["endCursor"]

    def preview_seat_assignment(self, user_uid: str, product: SeatProduct) -> SeatAssignmentPreview:
        """
        What assigning a seat to a user would do: whether it buys a seat, and what that
        costs today. Needs the users:admin:write permission.

        Args:
            user_uid (str): The ID of the user.
            product (SeatProduct): The seat product.

        Returns:
            SeatAssignmentPreview: The pool, and the purchase the assignment needs.
        """

        query = (
            """
            query seatAssignmentPreview($uid: String!, $product: SeatProduct!) {
                seatAssignmentPreview(uid: $uid, product: $product) {
                    alreadyAssigned
                    needsPurchase
                    seatsAfter
                    prorationCents
                    seatPool { """
            + _POOL_FIELDS
            + """ }
                }
            }
        """
        )

        preview = self.client._call_gql(
            query=query, variables={"uid": user_uid, "product": product.value}
        )["seatAssignmentPreview"]
        return SeatAssignmentPreview(
            pool=_pool(preview["seatPool"], []),
            already_assigned=preview["alreadyAssigned"],
            needs_purchase=preview["needsPurchase"],
            seats_after=preview["seatsAfter"],
            proration_cents=preview["prorationCents"],
        )

    def assign_seat(
        self,
        user_uid: str,
        product: SeatProduct,
        *,
        confirm_purchase: bool = False,
        expected_seats_after: Optional[int] = None,
    ) -> SeatAssignment:
        """
        Gives a user of the workspace a seat on a product. Needs the users:admin:write
        permission, which can buy seats.

        When the pool is full nothing is bought and ``ParadimeAPIException`` is raised
        with a message starting ``PURCHASE_CONFIRMATION_REQUIRED``. Call
        ``preview_seat_assignment`` for the cost, then assign again with
        ``confirm_purchase=True`` and ``expected_seats_after=preview.seats_after``.
        Other refusals raise the same way, their message starting with the reason, such as
        ``ROLE_CANNOT_USE_PRODUCT``.

        Args:
            user_uid (str): The ID of the user.
            product (SeatProduct): The seat product.
            confirm_purchase (bool): Buy a seat when the pool is full.
            expected_seats_after (Optional[int]): The ``seats_after`` being confirmed.

        Returns:
            SeatAssignment: The pool after the assignment, and the user's seat.
        """

        query = (
            """
            mutation assignSeat($input: AssignSeatInput!) {
                assignSeat(input: $input) {
                    assigned
                    purchased
                    holder { uid name email assignedAt }
                    seatPool { """
            + _POOL_FIELDS
            + """ }
                }
            }
        """
        )

        result = self.client._call_gql(
            query=query,
            variables={
                "input": {
                    "uid": user_uid,
                    "product": product.value,
                    "confirmPurchase": confirm_purchase,
                    "expectedSeatsAfter": expected_seats_after,
                }
            },
        )["assignSeat"]
        return SeatAssignment(
            pool=_pool(result["seatPool"], []),
            holder=_holder(result["holder"]) if result["holder"] else None,
            assigned=result["assigned"],
            purchased=result["purchased"],
        )

    def unassign_seat(self, user_uid: str, product: SeatProduct) -> bool:
        """
        Takes back the seat of a user of the workspace; the seat stays in the pool.
        Needs the users:admin:write permission.

        Args:
            user_uid (str): The ID of the user.
            product (SeatProduct): The seat product.

        Returns:
            bool: False when the user held no seat.
        """

        query = """
            mutation unassignSeat($input: UnassignSeatInput!) {
                unassignSeat(input: $input) {
                    freed
                }
            }
        """

        return bool(
            self.client._call_gql(
                query=query,
                variables={"input": {"uid": user_uid, "product": product.value}},
            )["unassignSeat"]["freed"]
        )


_POOL_FIELDS = (
    "product seats scheduledSeats scheduledFrom paidSeats contractedSeats assigned available"
)


def _holder(node: Dict[str, Any]) -> SeatHolder:
    return SeatHolder(
        uid=node["uid"], name=node["name"], email=node["email"], assigned_at=node["assignedAt"]
    )


def _pool(pool: Dict[str, Any], holders: List[SeatHolder]) -> SeatPool:
    return SeatPool(
        product=pool["product"],
        seats=pool["seats"],
        scheduled_seats=pool["scheduledSeats"],
        scheduled_from=pool["scheduledFrom"],
        paid_seats=pool["paidSeats"],
        contracted_seats=pool["contractedSeats"],
        assigned=pool["assigned"],
        available=pool["available"],
        holders=holders,
    )

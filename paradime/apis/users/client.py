from typing import List, Optional

from paradime.apis.users.types import (
    ActiveUser,
    InvitedUser,
    SeatAssignment,
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
        Retrieves the company's seat pool for a product and who holds each seat.
        Needs the users:read permission.

        Args:
            product (SeatProduct): The seat product.

        Returns:
            SeatPool: The pool and its holders.
        """

        query = """
            query listSeats($product: SeatProduct!) {
                listSeats(product: $product) {
                    product
                    seats
                    scheduledSeats
                    scheduledFrom
                    paidSeats
                    contractedSeats
                    assigned
                    available
                    holders { uid name email assignedAt }
                }
            }
        """

        pool = self.client._call_gql(query=query, variables={"product": product.value})["listSeats"]
        return SeatPool(
            product=pool["product"],
            seats=pool["seats"],
            scheduled_seats=pool["scheduledSeats"],
            scheduled_from=pool["scheduledFrom"],
            paid_seats=pool["paidSeats"],
            contracted_seats=pool["contractedSeats"],
            assigned=pool["assigned"],
            available=pool["available"],
            holders=[
                SeatHolder(
                    uid=holder["uid"],
                    name=holder["name"],
                    email=holder["email"],
                    assigned_at=holder["assignedAt"],
                )
                for holder in pool["holders"]
            ],
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
        Gives a user a seat on a product. Needs the users:write permission.

        When the pool is full nothing is bought: the result has ``ok=False``,
        ``needs_purchase=True``, ``seats_after`` and ``proration_cents``. Call again with
        ``confirm_purchase=True`` and ``expected_seats_after=result.seats_after`` to buy
        the seat and assign it.

        Args:
            user_uid (str): The ID of the user.
            product (SeatProduct): The seat product.
            confirm_purchase (bool): Buy a seat when the pool is full.
            expected_seats_after (Optional[int]): The ``seats_after`` being confirmed.

        Returns:
            SeatAssignment: The outcome.
        """

        query = """
            mutation assignSeat(
                $uid: String!,
                $product: SeatProduct!,
                $confirmPurchase: Boolean,
                $expectedSeatsAfter: Int
            ) {
                assignSeat(
                    uid: $uid,
                    product: $product,
                    confirmPurchase: $confirmPurchase,
                    expectedSeatsAfter: $expectedSeatsAfter
                ) {
                    ok
                    needsPurchase
                    assigned
                    purchased
                    seats
                    seatsAfter
                    prorationCents
                }
            }
        """

        result = self.client._call_gql(
            query=query,
            variables={
                "uid": user_uid,
                "product": product.value,
                "confirmPurchase": confirm_purchase,
                "expectedSeatsAfter": expected_seats_after,
            },
        )["assignSeat"]
        return SeatAssignment(
            ok=result["ok"],
            needs_purchase=result["needsPurchase"],
            assigned=result["assigned"],
            purchased=result["purchased"],
            seats=result["seats"],
            seats_after=result["seatsAfter"],
            proration_cents=result["prorationCents"],
        )

    def unassign_seat(self, user_uid: str, product: SeatProduct) -> bool:
        """
        Takes a user's seat back; the seat stays in the pool. Needs the users:write
        permission.

        Args:
            user_uid (str): The ID of the user.
            product (SeatProduct): The seat product.

        Returns:
            bool: False when the user held no seat.
        """

        query = """
            mutation unassignSeat($uid: String!, $product: SeatProduct!) {
                unassignSeat(uid: $uid, product: $product) {
                    freed
                }
            }
        """

        return bool(
            self.client._call_gql(
                query=query, variables={"uid": user_uid, "product": product.value}
            )["unassignSeat"]["freed"]
        )

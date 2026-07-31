from typing import List

from paradime.apis.users.types import ActiveUser, InvitedUser, UserAccountType
from paradime.client.api_client import APIClient
from paradime.graphql import load_operation
from paradime.tools.pydantic import parse_obj_as


class UsersClient:
    def __init__(self, client: APIClient):
        self.client = client

    def list_active(self) -> List[ActiveUser]:
        """
        Retrieves all active users.

        Returns:
            List[ActiveUser]: A list of active user objects.
        """

        query = load_operation("users", "list_active")

        response = self.client._call_gql(query)
        return parse_obj_as(List[ActiveUser], response["listUsers"]["activeUsers"])

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

        query = load_operation("users", "list_invited")

        response = self.client._call_gql(query)
        return parse_obj_as(List[InvitedUser], response["listUsers"]["invitedUsers"])

    def invite(self, email: str, account_type: UserAccountType) -> None:
        """
        Invites a user to the workspace.

        Args:
            email (str): The email of the user to invite.
            account_type (UserAccountType): The account type of the user to invite.
        """

        query = load_operation("users", "invite")

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

        query = load_operation("users", "update_account_type")

        self.client._call_gql(
            query=query,
            variables={"uid": user_uid, "accountType": account_type.value},
        )

    def disable(self, user_uid: str) -> None:
        query = load_operation("users", "disable")

        self.client._call_gql(query=query, variables={"uid": user_uid})

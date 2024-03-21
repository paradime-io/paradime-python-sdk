from typing import List

from paradime.apis.users.types import ActiveUser, InvitedUser, UserAccountType
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

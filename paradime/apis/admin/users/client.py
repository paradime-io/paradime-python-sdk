from typing import Iterator, List, Optional, Sequence

from paradime.apis.admin.transport import AdminTransport
from paradime.apis.admin.types import (
    CONNECTION_FIELDS,
    DEFAULT_PAGE_SIZE,
    Page,
    check_page_size,
    iterate_pages,
    parse_page,
    serialize_input,
)
from paradime.apis.admin.users.types import (
    USER_FIELDS,
    AdminUser,
    CreateUserInput,
    DeactivateUserInput,
    ReactivateUserInput,
    SetAutoJoinDomainsInput,
    UpdateUserInput,
    UserAccountType,
    UserStatus,
)
from paradime.tools.pydantic import BaseModel


class AdminUsersClient:
    """
    A workspace's members, former members and invitees.
    """

    def __init__(self, transport: AdminTransport) -> None:
        self.transport = transport

    def list(
        self,
        *,
        statuses: Optional[Sequence[UserStatus]] = None,
        workspace_uid: Optional[str] = None,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> Iterator[AdminUser]:
        """
        Iterates over the workspace's users, ordered by email, then status.

        Args:
            statuses (Sequence[UserStatus], optional): Only users with one of these statuses.
            workspace_uid (str, optional): The workspace to read, for a company API key.
            page_size (int): How many users to read per request, at most 500.

        Returns:
            Iterator[AdminUser]: The users, read a page at a time.
        """

        check_page_size(page_size)
        query = f"""
            query adminUsers($status: [UserStatus!], $first: Int, $after: String) {{
                users(status: $status, first: $first, after: $after) {{
                    edges {{
                        node {{
                            {USER_FIELDS}
                        }}
                    }}
                    {CONNECTION_FIELDS}
                }}
            }}
        """
        status = [UserStatus(value).value for value in statuses] if statuses else None

        def fetch_page(after: Optional[str]) -> Page[AdminUser]:
            response = self.transport.execute(
                query,
                {"status": status, "first": page_size, "after": after},
                workspace_uid=workspace_uid,
            )
            return parse_page(response["users"], AdminUser.parse_obj)

        return iterate_pages(fetch_page)

    def create(
        self,
        email: str,
        account_type: UserAccountType,
        *,
        workspace_uid: Optional[str] = None,
    ) -> AdminUser:
        """
        Invites a person by email with a role, and sends them the invitation email. An email
        that already belongs to a user or to a pending invitation raises
        `AdminConflictException`.

        Args:
            email (str): The email the invitation is sent to.
            account_type (UserAccountType): The role the invitation gives.
            workspace_uid (str, optional): The workspace to change, for a company API key.

        Returns:
            AdminUser: The invitee, `INVITED` with a `PENDING` invitation.
        """

        return self._mutate(
            "createUser",
            CreateUserInput(email=email, account_type=account_type),
            workspace_uid,
        )

    def update(
        self,
        uid: str,
        *,
        account_type: UserAccountType,
        workspace_uid: Optional[str] = None,
    ) -> AdminUser:
        """
        Changes the role of an active member. Giving a user the role they hold is not an error.

        Args:
            uid (str): The user's uid.
            account_type (UserAccountType): The role the user gets.
            workspace_uid (str, optional): The workspace to change, for a company API key.

        Returns:
            AdminUser: The user, with their new role.
        """

        return self._mutate(
            "updateUser", UpdateUserInput(uid=uid, account_type=account_type), workspace_uid
        )

    def deactivate(self, uid: str, *, workspace_uid: Optional[str] = None) -> AdminUser:
        """
        Deactivates a member: they can no longer sign in to the workspace, and `reactivate`
        brings them back with the same role. Deactivating a former member is not an error.

        Args:
            uid (str): The user's uid.
            workspace_uid (str, optional): The workspace to change, for a company API key.

        Returns:
            AdminUser: The user, deactivated.
        """

        return self._mutate("deactivateUser", DeactivateUserInput(uid=uid), workspace_uid)

    def reactivate(self, uid: str, *, workspace_uid: Optional[str] = None) -> AdminUser:
        """
        Brings a former member back with the role they had. Reactivating an active user is
        not an error.

        Args:
            uid (str): The user's uid.
            workspace_uid (str, optional): The workspace to change, for a company API key.

        Returns:
            AdminUser: The user, active again.
        """

        return self._mutate("reactivateUser", ReactivateUserInput(uid=uid), workspace_uid)

    def get_auto_join_domains(self, *, workspace_uid: Optional[str] = None) -> List[str]:
        """
        Reads the email domains whose users join the workspace on sign-up.

        Args:
            workspace_uid (str, optional): The workspace to read, for a company API key.

        Returns:
            List[str]: The domains; empty when auto-join is off.
        """

        query = """
            query adminAutoJoinDomains($workspaceUid: String) {
                workspace(workspaceUid: $workspaceUid) {
                    autoJoinDomains
                }
            }
        """

        response = self.transport.execute(
            query, {"workspaceUid": workspace_uid}, workspace_uid=workspace_uid
        )
        return (response["workspace"] or {}).get("autoJoinDomains") or []

    def set_auto_join_domains(
        self, domains: Sequence[str], *, workspace_uid: Optional[str] = None
    ) -> List[str]:
        """
        Replaces the workspace's auto-join domains. An empty list turns auto-join off.

        Args:
            domains (Sequence[str]): Email domains, such as ``example.com``.
            workspace_uid (str, optional): The workspace to change, for a company API key.

        Returns:
            List[str]: The domains now set.
        """

        query = """
            mutation adminSetAutoJoinDomains($input: SetAutoJoinDomainsInput!) {
                setAutoJoinDomains(input: $input) {
                    workspace {
                        autoJoinDomains
                    }
                }
            }
        """

        response = self.transport.execute(
            query,
            {"input": serialize_input(SetAutoJoinDomainsInput(domains=list(domains)))},
            workspace_uid=workspace_uid,
        )
        return response["setAutoJoinDomains"]["workspace"].get("autoJoinDomains") or []

    def _mutate(
        self, mutation: str, mutation_input: BaseModel, workspace_uid: Optional[str]
    ) -> AdminUser:
        input_type = mutation[0].upper() + mutation[1:] + "Input"
        query = f"""
            mutation admin{input_type[:-5]}($input: {input_type}!) {{
                {mutation}(input: $input) {{
                    user {{
                        {USER_FIELDS}
                    }}
                }}
            }}
        """

        response = self.transport.execute(
            query, {"input": serialize_input(mutation_input)}, workspace_uid=workspace_uid
        )
        return AdminUser.parse_obj(response[mutation]["user"])

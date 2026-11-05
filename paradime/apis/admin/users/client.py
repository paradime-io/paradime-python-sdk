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
    ReactivateUserInput,
    SetAutoJoinDomainsInput,
    UserStatus,
)


class AdminUsersClient:
    """
    A workspace's members, former members and invitees. Inviting, changing roles and disabling
    users stay on `paradime.users`.
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
            workspace_uid (str, optional): The workspace to read, for a company-level key.
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

    def reactivate(self, uid: str, *, workspace_uid: Optional[str] = None) -> AdminUser:
        """
        Brings a former member back with the role they had. Reactivating an active user is
        not an error.

        Args:
            uid (str): The user's uid.
            workspace_uid (str, optional): The workspace to change, for a company-level key.

        Returns:
            AdminUser: The user, active again.
        """

        query = f"""
            mutation adminReactivateUser($input: ReactivateUserInput!) {{
                reactivateUser(input: $input) {{
                    user {{
                        {USER_FIELDS}
                    }}
                }}
            }}
        """

        response = self.transport.execute(
            query,
            {"input": serialize_input(ReactivateUserInput(uid=uid))},
            workspace_uid=workspace_uid,
        )
        return AdminUser.parse_obj(response["reactivateUser"]["user"])

    def get_auto_join_domains(self, *, workspace_uid: Optional[str] = None) -> List[str]:
        """
        Reads the email domains whose users join the workspace on sign-up.

        Args:
            workspace_uid (str, optional): The workspace to read, for a company-level key.

        Returns:
            List[str]: The domains; empty when auto-join is off.
        """

        query = """
            query adminAutoJoinDomains($workspaceUid: String) {
                workspace(uid: $workspaceUid) {
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
            workspace_uid (str, optional): The workspace to change, for a company-level key.

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

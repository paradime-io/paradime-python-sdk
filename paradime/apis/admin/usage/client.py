from datetime import datetime
from typing import Iterator, Optional

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
from paradime.apis.admin.usage.types import (
    USAGE_TOTALS_FIELDS,
    Usage,
    UsagePeriodInput,
    WorkspaceUsage,
)


class AdminUsageClient:
    def __init__(self, transport: AdminTransport) -> None:
        self.transport = transport

    def get(
        self,
        period_from: datetime,
        period_to: datetime,
        *,
        workspace_uid: Optional[str] = None,
    ) -> Optional[Usage]:
        """
        Reads Bolt and DinoAI usage in a period, summed over the workspaces asked for.

        Args:
            period_from (datetime): When the period starts, included. A naive time is UTC.
            period_to (datetime): When the period ends, excluded. At most 366 days after
                `period_from`.
            workspace_uid (str, optional): One workspace to sum. Leave it out for every
                workspace the key can reach.

        Returns:
            Optional[Usage]: The usage, or None when the server returned none.
        """

        query = f"""
            query adminUsage($period: UsagePeriodInput!, $workspaceUid: String) {{
                usage(period: $period, workspaceUid: $workspaceUid) {{
                    from
                    to
                    {USAGE_TOTALS_FIELDS}
                }}
            }}
        """

        period = UsagePeriodInput(period_from=period_from, period_to=period_to)
        response = self.transport.execute(
            query, {"period": serialize_input(period), "workspaceUid": workspace_uid}
        )
        usage = response["usage"]
        return Usage.parse_obj(usage) if usage is not None else None

    def list_workspaces(
        self,
        period_from: datetime,
        period_to: datetime,
        *,
        workspace_uid: Optional[str] = None,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> Iterator[WorkspaceUsage]:
        """
        Iterates over a period's usage one workspace at a time, ordered by workspace uid.

        Args:
            period_from (datetime): When the period starts, included.
            period_to (datetime): When the period ends, excluded.
            workspace_uid (str, optional): Only this workspace.
            page_size (int): How many workspaces to read per request, at most 500.

        Returns:
            Iterator[WorkspaceUsage]: Each workspace's usage, read a page at a time.
        """

        check_page_size(page_size)
        query = f"""
            query adminWorkspaceUsage(
                $period: UsagePeriodInput!
                $workspaceUid: String
                $first: Int
                $after: String
            ) {{
                usage(period: $period, workspaceUid: $workspaceUid) {{
                    workspaces(first: $first, after: $after) {{
                        edges {{
                            node {{
                                workspace {{
                                    uid
                                    name
                                }}
                                {USAGE_TOTALS_FIELDS}
                            }}
                        }}
                        {CONNECTION_FIELDS}
                    }}
                }}
            }}
        """
        period = serialize_input(UsagePeriodInput(period_from=period_from, period_to=period_to))

        def fetch_page(after: Optional[str]) -> Page[WorkspaceUsage]:
            response = self.transport.execute(
                query,
                {
                    "period": period,
                    "workspaceUid": workspace_uid,
                    "first": page_size,
                    "after": after,
                },
            )
            usage = response["usage"] or {}
            return parse_page(usage.get("workspaces"), WorkspaceUsage.parse_obj)

        return iterate_pages(fetch_page)

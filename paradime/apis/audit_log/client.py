from typing import List

from paradime.apis.audit_log.types import AuditLog
from paradime.client.api_client import APIClient
from paradime.graphql import load_operation
from paradime.tools.pydantic import parse_obj_as


class AuditLogClient:
    def __init__(self, client: APIClient):
        self.client = client

    def get_all(self) -> List[AuditLog]:
        """
        Retrieves all audit logs.

        Returns:
            List[AuditLog]: A list of audit log objects.
        """
        query = load_operation("audit_log", "get_all")
        response = self.client._call_gql(query)
        return parse_obj_as(List[AuditLog], response["getAuditLogs"]["auditLogs"])

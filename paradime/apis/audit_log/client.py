from typing import List

from paradime.apis.audit_log.types import AuditLog
from paradime.client.api_client import APIClient
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
        query = """
            query GetAuditLogs {
                getAuditLogs {
                    auditLogs {
                        id
                        createdDttm
                        updatedDttm
                        workspaceId
                        workspaceName
                        actorType
                        actorUserId
                        actorEmail
                        eventSourceId
                        eventSource
                        eventId
                        eventType
                        metadataJson
                    }
                }
            }
        """
        response = self.client._call_gql(query)
        return parse_obj_as(List[AuditLog], response["getAuditLogs"]["auditLogs"])

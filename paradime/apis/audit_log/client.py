from typing import List

from paradime.apis.audit_log.types import AuditLog
from paradime.client.api_client import APIClient


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
        return [
            AuditLog(
                id=audit_log["id"],
                created_dttm=audit_log["createdDttm"],
                updated_dttm=audit_log["updatedDttm"],
                workspace_id=audit_log["workspaceId"],
                workspace_name=audit_log["workspaceName"],
                actor_type=audit_log["actorType"],
                actor_user_id=audit_log["actorUserId"],
                actor_email=audit_log["actorEmail"],
                event_source_id=audit_log["eventSourceId"],
                event_source=audit_log["eventSource"],
                event_id=audit_log["eventId"],
                event_type=audit_log["eventType"],
                metadata_json=audit_log["metadataJson"],
            )
            for audit_log in response["getAuditLogs"]["auditLogs"]
        ]

from typing import List

from pydantic import parse_obj_as

from paradime.apis.workspaces.types import Workspace
from paradime.client.api_client import APIClient


class WorkspacesClient:
    def __init__(self, client: APIClient):
        self.client = client

    def list_all(self) -> List[Workspace]:
        """
        Retrieves all active workspaces.

        Returns:
            List[Workspace]: A list of active workspaces
        """

        query = """
            query listWorkspaces {
                listWorkspaces{
                    workspaces{
                        uid
                        name
                    }
                }
            }
        """

        response = self.client._call_gql(query)
        return parse_obj_as(List[Workspace], response["listWorkspaces"]["workspaces"])

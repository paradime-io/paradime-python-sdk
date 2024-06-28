from paradime.tools.pydantic import BaseModel


class Workspace(BaseModel):
    name: str
    uid: str

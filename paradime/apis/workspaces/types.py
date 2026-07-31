from paradime.tools.models import ParadimeResponseModel


class Workspace(ParadimeResponseModel):
    name: str
    uid: str

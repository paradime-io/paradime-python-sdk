class ParadimeException(Exception):
    """
    Base exception for the Paradime API client.
    """

    pass


class ParadimeAPIException(ParadimeException):
    """
    Exception for errors in the Paradime API.
    """

    pass


class ParadimeWorkspaceRequiredException(ParadimeAPIException):
    """
    A workspace-level call was made with a company-level API token and no `workspace_uid`.
    """

    pass

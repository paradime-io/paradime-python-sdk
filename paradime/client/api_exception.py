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

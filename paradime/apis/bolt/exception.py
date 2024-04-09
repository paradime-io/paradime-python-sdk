from paradime.client.api_exception import ParadimeException


class BoltScheduleLatestRunNotFoundException(ParadimeException):
    """
    Exception for when the latest run for a Bolt schedule is not found.
    """

    pass


class BoltScheduleArtifactNotFoundException(ParadimeException):
    """
    Exception for when the artifact for a Bolt schedule is not found.
    """

    pass

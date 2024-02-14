import uuid


def generate_uid(name: str) -> str:
    """
    Generate a unique identifier (UID) based on the given name.

    Args:
        name (str): The name to generate the UID from.

    Returns:
        str: The generated UID as a string.
    """
    uid = uuid.uuid5(uuid.NAMESPACE_DNS, name)
    return str(uid)

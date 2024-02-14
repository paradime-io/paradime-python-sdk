import uuid


def generate_uid(name: str) -> str:
    uid = uuid.uuid5(uuid.NAMESPACE_DNS, name)
    return str(uid)

from packaging import version
from pydantic import VERSION

if version.parse(VERSION) >= version.parse("2.0.0"):
    from pydantic.v1 import *
else:
    from pydantic import *

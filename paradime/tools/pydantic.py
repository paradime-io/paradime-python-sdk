from packaging import version
from pydantic import VERSION  # noqa: PYD002

if version.parse(VERSION) >= version.parse("2.0.0"):
    from pydantic.v1 import *  # type: ignore # noqa: PYD002, F403, F401
else:
    from pydantic import *  # type: ignore # noqa: PYD002, F403, F401

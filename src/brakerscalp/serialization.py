from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any

import orjson
from pydantic import BaseModel


def _default(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, Enum):
        return value.value
    raise TypeError(f"Unsupported type: {type(value)!r}")


def dumps(value: Any) -> bytes:
    return orjson.dumps(value, default=_default)


def loads(value: bytes | str | None) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.encode("utf-8")
    return orjson.loads(value)


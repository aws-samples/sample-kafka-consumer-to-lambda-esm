"""Projection decisions. No I/O.

Nothing here touches AWS or reads a clock, which makes every decision testable offline.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .records import Record

# The event's own notion of when it happened, distinct from the Kafka record timestamp, which is
# when the broker received it. The two diverge on replay, and precedence uses the event's own.
SOURCE_TIMESTAMP_FIELD = "sourceTimestamp"
BUSINESS_KEY_FIELD = "id"
EVENT_TYPE_FIELD = "eventType"


@dataclass(frozen=True)
class Projection:
    """What a record means for the read model, decided without touching a datastore."""

    business_key: str
    source_timestamp: int
    attributes: dict[str, Any]
    removed: bool


def source_timestamp(record: Record, payload: dict[str, Any]) -> int:
    """The precedence value for this record.

    Fails open: a payload with no source timestamp falls back to the Kafka record timestamp rather
    than being rejected. Events written before a precedence field existed carry none, and refusing
    them would strand historical data.

    The cost is that such records cannot be ordered against each other, so a replay covering them
    can overwrite newer state. An in-place replay should quantify that population first.
    """
    raw = payload.get(SOURCE_TIMESTAMP_FIELD)
    if raw is None:
        return record.timestamp_ms
    return int(raw)


def business_key(payload: dict[str, Any]) -> str:
    key = payload.get(BUSINESS_KEY_FIELD)
    if not key:
        raise ValueError(f"payload has no {BUSINESS_KEY_FIELD!r} field")
    return str(key)


def event_type(payload: dict[str, Any]) -> str | None:
    """The discriminator the event source mapping filters on.

    Read from the payload value, not from a Kafka header. Filter criteria match the value only, so
    a type that exists solely in a header is invisible to the filter: the record is delivered and
    this function returns None. The header_only scenario exercises that case.
    """
    value = payload.get(EVENT_TYPE_FIELD)
    return str(value) if value is not None else None


def project(record: Record, payload: dict[str, Any]) -> Projection:
    """Decide the read-model effect of one record. Pure."""
    removed = event_type(payload) in {"OrderCancelled", "ShipmentCancelled"}

    attributes = {
        k: v
        for k, v in payload.items()
        if k not in {BUSINESS_KEY_FIELD, SOURCE_TIMESTAMP_FIELD}
    }

    return Projection(
        business_key=business_key(payload),
        source_timestamp=source_timestamp(record, payload),
        attributes=attributes,
        removed=removed,
    )


def is_stale(incoming: int, stored: int | None) -> bool:
    """Whether an incoming event should be discarded as older than stored state.

    The remove and cancel paths are gated by this as well. A removal applied without a precedence
    check lets a stale cancellation regress newer state, which is the same failure as an
    out-of-order update.

    Equal timestamps count as stale: the record has already been applied.
    """
    if stored is None:
        return False
    return incoming <= stored

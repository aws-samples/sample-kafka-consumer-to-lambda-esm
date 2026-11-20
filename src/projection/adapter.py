"""Event source mapping adapter. The only module that knows the ESM payload shape.

Everything below this in the call graph works on Record objects, so an archive-driven replay can
reuse the projection instead of reimplementing it.
"""

from __future__ import annotations

from typing import Any

from .records import Record, _decode_b64, _decode_headers


def records_from_esm_event(event: dict[str, Any]) -> list[Record]:
    """Flatten an ESM event into records, in per-partition offset order.

    The ESM groups records under a `records` map keyed by "topic-partition", in offset order. The
    sort below makes that explicit rather than relying on it, because per-partition order is the
    one ordering guarantee this pattern depends on.

    Records from different partitions have no defined order relative to each other, as in Kafka.

    """
    out: list[Record] = []

    for topic_partition, raw_records in (event.get("records") or {}).items():
        for raw in raw_records:
            out.append(
                Record(
                    topic=raw["topic"],
                    partition=int(raw["partition"]),
                    offset=int(raw["offset"]),
                    timestamp_ms=int(raw.get("timestamp") or 0),
                    key=_decode_b64(raw.get("key")),
                    value=_decode_b64(raw["value"]) or "",
                    headers=_decode_headers(raw.get("headers")),
                )
            )

    out.sort(key=lambda r: (r.topic, r.partition, r.offset))
    return out

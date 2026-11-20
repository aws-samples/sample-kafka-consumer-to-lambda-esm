"""Demo scenario payloads. Pure data, no Kafka client.

Separated from handler.py so the payload shapes can be tested offline. Nothing here imports kafka
or boto3, which keeps the test suite dependency-free.

Each builder returns (records, expectation), where records are (key, value, headers) triples. The
expectation string is returned in the invocation response so the caller knows what to look for.
"""

from __future__ import annotations

import time
import uuid
from typing import Any, List, Optional, Tuple

# Spelled with typing constructs rather than `tuple[...] | None`. A module-level type alias is
# evaluated at import time, so `from __future__ import annotations` does not defer it, and the PEP
# 604 form raises on anything below Python 3.10. The Lambda runtime is 3.12, but the test
# suite should run on whatever interpreter a reader already has, so the package stays 3.9-clean.
Record = Tuple[str, Any, Optional[List[Tuple[str, bytes]]]]


def _now_ms() -> int:
    return int(time.time() * 1000)


def _event(
    *,
    business_key: str,
    event_type: str | None,
    source_timestamp: int | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {"id": business_key}
    if event_type is not None:
        body["eventType"] = event_type
    if source_timestamp is not None:
        body["sourceTimestamp"] = source_timestamp
    body.update(extra or {})
    return body


def matching(topic: str, event_type: str) -> Tuple[List[Record], str]:
    key = f"order-{uuid.uuid4().hex[:8]}"
    return (
        [(key, _event(business_key=key, event_type=event_type, source_timestamp=_now_ms()), None)],
        f"Expect one invocation and one read-model item for {key}.",
    )


def non_matching(topic: str, event_type: str) -> Tuple[List[Record], str]:
    """A record the filter rejects. Baseline for the header_only scenario."""
    key = f"order-{uuid.uuid4().hex[:8]}"
    return (
        [
            (
                key,
                _event(business_key=key, event_type="SomethingElse", source_timestamp=_now_ms()),
                None,
            )
        ],
        "Expect no invocation. The record is filtered out before the function and offsets advance "
        "past it, so no backlog accumulates behind the filter.",
    )


def header_only(topic: str, event_type: str) -> Tuple[List[Record], str]:
    """The event type is set in a Kafka header, not in the payload.

    Filter criteria match the message value only, so this record never reaches the function and
    leaves no log entry. FilteredOutEventCount is the only signal that it existed.
    """
    key = f"order-{uuid.uuid4().hex[:8]}"
    return (
        [
            (
                key,
                # Deliberately no eventType in the body.
                _event(business_key=key, event_type=None, source_timestamp=_now_ms()),
                [("eventType", event_type.encode())],
            )
        ],
        "Expect no invocation even though the header type matches, because filtering reads the "
        "value only. Confirm with FilteredOutEventCount; the function log shows nothing.",
    )


def poison(topic: str, event_type: str) -> Tuple[List[Record], str]:
    """A record that passes the filter but breaks the handler.

    The eventType is visible to the filter but the business key is missing, so the handler raises.
    The record is reported as a batch item failure, retried to the mapping's budget, then delivered
    to S3 with its full payload.
    """
    return (
        [
            (
                "poison",
                {"eventType": event_type, "sourceTimestamp": _now_ms(), "note": "no id field"},
                None,
            )
        ],
        "Expect failing invocations, then one object in the failure bucket containing the full "
        "record including its payload. An SQS destination would carry offsets only.",
    )


def isolation(topic: str, event_type: str) -> Tuple[List[Record], str]:
    """One poison record among processable ones.

    With ReportBatchItemFailures the good records are applied once and only the poison record is
    retried. Without it the whole batch retries and the good records are reprocessed each time.
    """
    now = _now_ms()
    records: List[Record] = []
    for i in range(5):
        key = f"iso-{uuid.uuid4().hex[:6]}-{i}"
        records.append(
            (key, _event(business_key=key, event_type=event_type, source_timestamp=now + i), None)
        )
    # Mid-batch, so trailing offsets exist behind the failure.
    records.insert(2, ("poison", {"eventType": event_type, "sourceTimestamp": now + 99}, None))
    return (
        records,
        "Expect 5 read-model items and 1 object in the failure bucket. In the ledger, the "
        "processable records should show attempts=1. Higher counts mean the whole batch was "
        "retried and per-record isolation is not in effect.",
    )


def stale(topic: str, event_type: str) -> Tuple[List[Record], str]:
    """Out-of-order delivery: newer first, then older, for the same business key.

    The precedence gate makes the older event a no-op through a conditional write. That gate is
    application logic; no service provides it.
    """
    key = f"order-{uuid.uuid4().hex[:8]}"
    now = _now_ms()
    return (
        [
            (
                key,
                _event(
                    business_key=key,
                    event_type=event_type,
                    source_timestamp=now,
                    extra={"status": "NEWER"},
                ),
                None,
            ),
            (
                key,
                _event(
                    business_key=key,
                    event_type=event_type,
                    source_timestamp=now - 600_000,
                    extra={"status": "OLDER"},
                ),
                None,
            ),
        ],
        f"Expect the read-model item for {key} to hold status=NEWER. Both records process and "
        "neither fails: the older one is a no-op because its conditional write lost. A superseded "
        "record must not be reported as a failure, or the mapping retries it against state that "
        "stays newer.",
    )


def ordering(topic: str, event_type: str) -> Tuple[List[Record], str]:
    """What happens to trailing offsets when one record in a batch fails.

    Retry resumes from the lowest reported failed offset, so records after it that already
    succeeded are redelivered. Delivery stays in offset order; the effects of those records repeat
    unless the handler is idempotent. Redelivery is bounded by the end of the failing batch rather
    than the end of the partition.
    """
    now = _now_ms()
    records: List[Record] = []
    for i in range(10):
        if i == 4:
            records.append(("poison", {"eventType": event_type, "sourceTimestamp": now + i}, None))
            continue
        key = f"ord-{i:02d}"
        records.append(
            (
                key,
                _event(
                    business_key=key,
                    event_type=event_type,
                    source_timestamp=now + i,
                    extra={"sequence": i},
                ),
                None,
            )
        )
    return (
        records,
        "From the function logs and the ledger, record: which offsets appear more than once, "
        "whether batches after the failure are redelivered, and the attempts count per record.",
    )


SCENARIOS = {
    "matching": matching,
    "non_matching": non_matching,
    "header_only": header_only,
    "poison": poison,
    "isolation": isolation,
    "stale": stale,
    "ordering": ordering,
}

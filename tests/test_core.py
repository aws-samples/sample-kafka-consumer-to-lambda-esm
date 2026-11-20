"""Pure-core tests: precedence, fail-open, and the header-vs-payload discriminator."""

from __future__ import annotations

import pytest

from projection.core import event_type, is_stale, project, source_timestamp
from projection.records import Record

from .events import esm_event, kafka_record
from projection.adapter import records_from_esm_event


def _record(**kwargs) -> Record:
    (record,) = records_from_esm_event(esm_event([kafka_record(**kwargs)]))
    return record


def test_source_timestamp_prefers_the_payload_field():
    record = _record(
        timestamp_ms=1_000,
        payload={"id": "o1", "eventType": "OrderPlaced", "sourceTimestamp": 9_999},
    )
    assert source_timestamp(record, record.payload()) == 9_999


def test_source_timestamp_fails_open_to_the_kafka_timestamp():
    """A payload with no precedence field must not be rejected.

    Failing open is a migration concession: events written before a precedence field existed
    carry none. The cost is that such records cannot be ordered against each other, which is
    why an in-place replay has to quantify this population first.
    """
    record = _record(timestamp_ms=4_242, payload={"id": "o1", "eventType": "OrderPlaced"})
    assert source_timestamp(record, record.payload()) == 4_242


def test_event_type_is_read_from_the_payload_not_headers():
    """The ESM filter can only match the value, so a header-only type is invisible to it.

    Such a record is delivered to the function rather than filtered out, and the core reports
    no event type. This is the behaviour demo D-1 makes observable.
    """
    record = _record(
        payload={"id": "o1", "sourceTimestamp": 1},
        headers={"eventType": "OrderPlaced"},
    )

    assert record.headers["eventType"] == "OrderPlaced"
    assert event_type(record.payload()) is None


@pytest.mark.parametrize(
    ("incoming", "stored", "expected"),
    [
        (100, None, False),  # nothing stored yet
        (200, 100, False),  # newer wins
        (100, 200, True),   # older is discarded
        (100, 100, True),   # equal means already applied
    ],
)
def test_is_stale(incoming, stored, expected):
    assert is_stale(incoming, stored) is expected


def test_cancellation_projects_as_removed():
    """The remove path carries a precedence value like any other mutation.

    A removal applied without a precedence check lets a stale cancellation regress newer state,
    which is the same failure mode as an out-of-order update.
    """
    record = _record(
        payload={"id": "o1", "eventType": "OrderCancelled", "sourceTimestamp": 500}
    )

    decision = project(record, record.payload())

    assert decision.removed is True
    assert decision.source_timestamp == 500
    assert decision.business_key == "o1"


def test_projection_strips_key_and_timestamp_from_attributes():
    record = _record(
        payload={"id": "o1", "eventType": "OrderPlaced", "sourceTimestamp": 5, "total": 42}
    )

    decision = project(record, record.payload())

    assert decision.attributes == {"eventType": "OrderPlaced", "total": 42}


def test_missing_business_key_is_an_error():
    record = _record(payload={"eventType": "OrderPlaced", "sourceTimestamp": 5})

    with pytest.raises(ValueError, match="'id'"):
        project(record, record.payload())

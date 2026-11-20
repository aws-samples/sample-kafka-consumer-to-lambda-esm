"""Adapter and record-shape tests. These pin the two ESM details that fail silently."""

from __future__ import annotations

from projection.adapter import records_from_esm_event
from projection.records import Record

from .events import esm_event, kafka_record


def test_decodes_base64_value_key_and_headers():
    event = esm_event(
        [kafka_record(key="order-7", headers={"eventType": "OrderPlaced", "trace": "abc"})]
    )

    (record,) = records_from_esm_event(event)

    assert record.topic == "orders"
    assert record.key == "order-7"
    assert record.payload()["eventType"] == "OrderPlaced"
    assert record.headers == {"eventType": "OrderPlaced", "trace": "abc"}


def test_records_are_sorted_by_partition_then_offset():
    event = esm_event(
        [
            kafka_record(partition=1, offset=5),
            kafka_record(partition=0, offset=9),
            kafka_record(partition=0, offset=2),
        ]
    )

    records = records_from_esm_event(event)

    assert [(r.partition, r.offset) for r in records] == [(0, 2), (0, 9), (1, 5)]


def test_empty_event_yields_no_records():
    assert records_from_esm_event({"records": {}}) == []
    assert records_from_esm_event({}) == []


def test_dedup_key_is_topic_partition_offset():
    record = Record(
        topic="orders", partition=3, offset=42, timestamp_ms=1, key=None, value="{}"
    )
    assert record.dedup_key == "orders#3#42"


def test_failure_identifier_is_an_object_not_a_string():
    """Kafka's partial batch response shape differs from every other event source.

    For Kinesis, DynamoDB Streams and SQS, itemIdentifier is a plain string. For Kafka it is an
    object of {partition: "<topic>-<partition>", offset: <int>}. Returning the wrong shape does
    not error: Lambda treats the identifier as absent from the batch and retries the WHOLE
    batch, silently undoing per-record isolation.
    """
    record = Record(
        topic="orders", partition=3, offset=42, timestamp_ms=1, key=None, value="{}"
    )

    identifier = record.failure_identifier

    assert identifier == {"partition": "orders-3", "offset": 42}
    assert isinstance(identifier, dict), "a bare string silently degrades to whole-batch retry"
    assert isinstance(identifier["offset"], int)

"""Batch-level behaviour: dedup, isolation, no-ops, and the attempt-ledger ordering."""

from __future__ import annotations

from projection.adapter import records_from_esm_event
from projection.handler import process_records

from .events import esm_event, kafka_record
from .fakes import ExplodingReadModel, FakeLedger, FakeReadModel


def _records(*raw):
    return records_from_esm_event(esm_event(list(raw)))


def test_clean_batch_reports_no_failures():
    records = _records(
        kafka_record(offset=1, payload={"id": "o1", "eventType": "OrderPlaced", "sourceTimestamp": 10}),
        kafka_record(offset=2, payload={"id": "o2", "eventType": "OrderPlaced", "sourceTimestamp": 11}),
    )
    ledger, read_model = FakeLedger(), FakeReadModel()

    failures, outcome = process_records(records, ledger, read_model)

    assert failures == []
    assert outcome.applied == 2
    assert set(read_model.items) == {"o1", "o2"}


def test_redelivered_record_is_suppressed_not_reapplied():
    """At-least-once delivery means this is normal traffic, not an error.

    Partial-batch retry resumes from the lowest reported failed offset, so records after it that
    already succeeded arrive again. Without the ledger they would be applied twice.
    """
    records = _records(
        kafka_record(offset=1, payload={"id": "o1", "eventType": "OrderPlaced", "sourceTimestamp": 10})
    )
    ledger, read_model = FakeLedger(), FakeReadModel()

    process_records(records, ledger, read_model)
    failures, outcome = process_records(records, ledger, read_model)

    assert failures == []
    assert outcome.duplicate == 1
    assert outcome.applied == 0
    assert read_model.calls == ["upsert:o1"], "second delivery must not touch the read model"


def test_attempt_is_counted_before_the_work_and_in_its_own_write():
    """begin_attempt must precede the projection write and must not share a transaction with it.

    A counter written after the business logic does not record the failures that matter: if the
    projection write fails and rolls back, a same-transaction counter rolls back with it, leaving
    the record looking untouched and eligible for retry indefinitely.

    Asserted here as call ordering. Enforced in DynamoLedger by being a separate UpdateItem rather
    than part of a TransactWriteItems.
    """
    records = _records(
        kafka_record(offset=1, payload={"id": "o1", "eventType": "OrderPlaced", "sourceTimestamp": 10})
    )
    ledger, read_model = FakeLedger(), ExplodingReadModel(fail_keys={"o1"})

    failures, outcome = process_records(records, ledger, read_model)

    assert ledger.calls == ["begin_attempt:orders#0#1"]
    assert ledger.attempts["orders#0#1"] == 1, "the failed attempt was still counted"
    assert outcome.failed == 1
    assert "mark_processed:orders#0#1" not in ledger.calls


def test_one_poison_record_does_not_fail_its_batch():
    """Per-record isolation: only the poison record is reported."""
    records = _records(
        kafka_record(offset=1, payload={"id": "good-1", "eventType": "OrderPlaced", "sourceTimestamp": 10}),
        kafka_record(offset=2, payload={"id": "poison", "eventType": "OrderPlaced", "sourceTimestamp": 11}),
        kafka_record(offset=3, payload={"id": "good-2", "eventType": "OrderPlaced", "sourceTimestamp": 12}),
    )
    ledger, read_model = FakeLedger(), ExplodingReadModel(fail_keys={"poison"})

    failures, outcome = process_records(records, ledger, read_model)

    assert failures == [{"itemIdentifier": {"partition": "orders-0", "offset": 2}}]
    assert outcome.applied == 2 and outcome.failed == 1
    assert read_model.applied == ["good-1", "good-2"]


def test_malformed_json_is_reported_as_a_failure():
    records = _records(kafka_record(offset=7, raw_value="{not json"))
    ledger, read_model = FakeLedger(), FakeReadModel()

    failures, outcome = process_records(records, ledger, read_model)

    assert failures == [{"itemIdentifier": {"partition": "orders-0", "offset": 7}}]
    assert outcome.failed == 1


def test_superseded_record_is_a_no_op_not_a_failure():
    """A losing precedence check must not be reported.

    Reporting it would make the mapping retry a record that is correctly being ignored, and
    since the stored state stays newer, that retry would fail the same way forever.
    """
    newer = _records(
        kafka_record(offset=1, payload={"id": "o1", "eventType": "OrderPlaced", "sourceTimestamp": 900})
    )
    older = _records(
        kafka_record(offset=2, payload={"id": "o1", "eventType": "OrderPlaced", "sourceTimestamp": 100})
    )
    ledger, read_model = FakeLedger(), FakeReadModel()

    process_records(newer, ledger, read_model)
    failures, outcome = process_records(older, ledger, read_model)

    assert failures == []
    assert outcome.superseded == 1
    assert read_model.items["o1"]["source_ts"] == 900, "newer state survived the older event"
    assert "mark_processed:orders#0#2" in ledger.calls


def test_stale_cancellation_cannot_regress_newer_state():
    """The removal path is precedence-gated like every other mutation."""
    update = _records(
        kafka_record(offset=1, payload={"id": "o1", "eventType": "OrderPlaced", "sourceTimestamp": 900})
    )
    stale_cancel = _records(
        kafka_record(offset=2, payload={"id": "o1", "eventType": "OrderCancelled", "sourceTimestamp": 100})
    )
    ledger, read_model = FakeLedger(), FakeReadModel()

    process_records(update, ledger, read_model)
    process_records(stale_cancel, ledger, read_model)

    assert read_model.items["o1"]["removed"] is False


def test_max_attempts_zero_never_parks():
    """The default leaves the ESM's retry budget and S3 destination as the only exit."""
    records = _records(
        kafka_record(offset=1, payload={"id": "poison", "eventType": "OrderPlaced", "sourceTimestamp": 10})
    )
    ledger, read_model = FakeLedger(), ExplodingReadModel(fail_keys={"poison"})

    for _ in range(5):
        failures, outcome = process_records(records, ledger, read_model, max_attempts=0)

    assert ledger.parked == {}
    assert outcome.failed == 1
    assert failures  # still reported, so the mapping keeps ownership of the record


def test_max_attempts_parks_after_the_threshold():
    records = _records(
        kafka_record(offset=1, payload={"id": "poison", "eventType": "OrderPlaced", "sourceTimestamp": 10})
    )
    ledger, read_model = FakeLedger(), ExplodingReadModel(fail_keys={"poison"})

    for _ in range(2):
        process_records(records, ledger, read_model, max_attempts=2)
    failures, outcome = process_records(records, ledger, read_model, max_attempts=2)

    assert outcome.parked == 1
    assert failures == [], "a parked record returns success so the offset advances"
    assert "orders#0#1" in ledger.parked


def test_records_from_different_partitions_are_independent():
    records = _records(
        kafka_record(partition=0, offset=1, payload={"id": "a", "eventType": "OrderPlaced", "sourceTimestamp": 10}),
        kafka_record(partition=1, offset=1, payload={"id": "poison", "eventType": "OrderPlaced", "sourceTimestamp": 10}),
    )
    ledger, read_model = FakeLedger(), ExplodingReadModel(fail_keys={"poison"})

    failures, outcome = process_records(records, ledger, read_model)

    assert failures == [{"itemIdentifier": {"partition": "orders-1", "offset": 1}}]
    assert outcome.applied == 1

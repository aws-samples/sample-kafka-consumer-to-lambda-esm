"""Scenario payload tests: offline, no Kafka client and no AWS.

These assert the payload SHAPES, because each demo only proves what it claims if the record it
produces has the right shape. `header_only` with an eventType accidentally left in the body would
pass the filter and quietly demonstrate nothing.
"""

from __future__ import annotations

import json

import pytest

from producer.scenarios import SCENARIOS


def _only(records):
    assert len(records) == 1
    return records[0]


def test_every_scenario_returns_records_and_an_expectation():
    for name, build in SCENARIOS.items():
        records, expectation = build("orders", "OrderPlaced")
        assert records, f"{name} produced no records"
        assert expectation.strip(), f"{name} has no expectation text"
        for key, value, _headers in records:
            assert isinstance(key, str) and key
            # Must be JSON-serialisable: ESM filtering only applies to UTF-8 JSON or plain
            # strings, so a payload that cannot round-trip through JSON could not be filtered.
            json.dumps(value)


def test_matching_carries_the_filtered_event_type_in_the_value():
    records, _ = SCENARIOS["matching"]("orders", "OrderPlaced")
    _key, value, headers = _only(records)

    assert value["eventType"] == "OrderPlaced", "filter matches the value, so it must be there"
    assert headers is None
    assert "id" in value and "sourceTimestamp" in value


def test_non_matching_has_a_different_event_type_in_the_value():
    records, _ = SCENARIOS["non_matching"]("orders", "OrderPlaced")
    _key, value, _headers = _only(records)

    assert value["eventType"] != "OrderPlaced"


def test_header_only_keeps_the_event_type_out_of_the_value():
    """The whole demo rests on this: eventType in the header, absent from the body.

    If it leaked into the value the filter would match, the function would be invoked, and the
    demo would prove the opposite of what it claims.
    """
    records, _ = SCENARIOS["header_only"]("orders", "OrderPlaced")
    _key, value, headers = _only(records)

    assert "eventType" not in value, "a value-side eventType would make the filter match"
    assert headers == [("eventType", b"OrderPlaced")]


def test_poison_passes_the_filter_but_breaks_the_handler():
    """It must reach the function, then fail: otherwise it tests filtering, not isolation."""
    records, _ = SCENARIOS["poison"]("orders", "OrderPlaced")
    _key, value, _headers = _only(records)

    assert value["eventType"] == "OrderPlaced", "must pass the filter to reach the handler"
    assert "id" not in value, "must lack the business key so the handler raises"


def test_isolation_places_the_poison_mid_batch():
    """Trailing offsets behind the failure are what make redelivery observable."""
    records, _ = SCENARIOS["isolation"]("orders", "OrderPlaced")

    poison_positions = [i for i, (_k, v, _h) in enumerate(records) if "id" not in v]

    assert len(records) == 6
    assert poison_positions == [2], "one poison record, neither first nor last"


def test_stale_sends_newer_before_older_for_one_key():
    records, _ = SCENARIOS["stale"]("orders", "OrderPlaced")

    assert len(records) == 2
    (key_a, first, _), (key_b, second, _) = records

    assert key_a == key_b, "precedence only applies within one business key"
    assert second["sourceTimestamp"] < first["sourceTimestamp"]
    assert first["status"] == "NEWER" and second["status"] == "OLDER"


def test_ordering_is_a_numbered_run_with_one_gap():
    records, expectation = SCENARIOS["ordering"]("orders", "OrderPlaced")

    good = [v for _k, v, _h in records if "id" in v]
    sequences = [v["sequence"] for v in good]

    assert len(records) == 10
    assert sequences == sorted(sequences), "must be produced in sequence order"
    assert 4 not in sequences, "position 4 is the poison record"
    assert "offsets" in expectation, "the expectation must tell the operator what to record"


@pytest.mark.parametrize("name", sorted(SCENARIOS))
def test_scenarios_do_not_import_a_kafka_client(name):
    """Proves the split: scenario data is testable without the producer's dependencies."""
    import sys

    assert "kafka" not in sys.modules
    assert "aws_msk_iam_sasl_signer" not in sys.modules

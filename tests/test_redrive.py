"""Redrive script tests: offline, using the real object shape captured from a live failure.

The fixture below is the actual structure of an S3 on-failure object, reduced but not reshaped:
three layers of wrapping, with base64 key and value at the bottom. Unwrapping it wrongly is the
easiest way to build a redrive script that appears to work and silently reprocesses nothing.
"""

from __future__ import annotations

import base64
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import importlib.util

_spec = importlib.util.spec_from_file_location(
    "redrive", Path(__file__).resolve().parents[1] / "scripts" / "redrive_from_s3.py"
)
redrive = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(redrive)


def _b64(text: str) -> str:
    return base64.b64encode(text.encode()).decode()


def _failure_object() -> bytes:
    """An S3 on-failure object, shaped exactly as the service writes it."""
    inner = {
        "records": {
            "orders-1": [
                {
                    "headers": [],
                    "key": _b64("poison"),
                    "offset": 3,
                    "partition": 1,
                    "timestamp": 1790621275576,
                    "timestampType": "CREATE_TIME",
                    "topic": "orders",
                    "value": _b64('{"eventType": "OrderPlaced", "sourceTimestamp": 1790621275035}'),
                },
                {
                    "headers": [],
                    "key": _b64("iso-42ade9-2"),
                    "offset": 4,
                    "partition": 1,
                    "timestamp": 1790621275581,
                    "timestampType": "CREATE_TIME",
                    "topic": "orders",
                    "value": _b64('{"id": "iso-42ade9-2", "eventType": "OrderPlaced"}'),
                },
            ]
        }
    }
    envelope = {
        "KafkaBatchInfo": {
            "batchSize": 2,
            "payloadSize": 1034,
            "recordsInfo": {
                "orders-1": {
                    "topic": "orders",
                    "partition": 1,
                    "firstRecordOffset": 3,
                    "lastRecordOffset": 4,
                }
            },
        },
        "requestContext": {"requestId": "4b7be9b4", "functionArn": "arn:aws:lambda:..."},
        "responseContext": {"statusCode": 200},
        "timestamp": "2026-09-28T18:47:58.790Z",
        "version": "1.0",
        # Layer 2: a JSON STRING, not a nested object.
        "payload": json.dumps(inner),
    }
    return json.dumps(envelope).encode()


def test_unwraps_all_three_layers():
    _envelope, records = redrive.unwrap(_failure_object())

    assert len(records) == 2
    poison, good = records

    assert poison["offset"] == 3
    assert poison["key"] == "poison"
    # The defect is visible in the decoded value, no `id` field. This is precisely what an SQS
    # destination could not have given you.
    assert "id" not in json.loads(poison["value"])

    assert good["key"] == "iso-42ade9-2"
    assert json.loads(good["value"])["id"] == "iso-42ade9-2"


def test_payload_as_a_string_is_parsed_not_treated_as_opaque():
    """Guards the layer that is easiest to get wrong.

    `payload` arrives as a string. Code that assumes a nested object gets a string back, finds no
    `records` key on it, and concludes there is nothing to redrive: producing a script that runs
    successfully and does nothing.
    """
    envelope = json.loads(_failure_object())

    assert isinstance(envelope["payload"], str), "the service really does send a string here"

    _meta, records = redrive.unwrap(_failure_object())
    assert records, "a string payload must still yield records"


def test_metadata_only_object_yields_nothing_redrivable():
    """What an SQS or SNS destination writes: coordinates, no payload.

    The script must report this rather than fail, since it is the shape produced by pointing at the
    wrong destination type. The useful output is that there is nothing here to redrive.
    """
    sqs_shaped = json.dumps(
        {
            "KafkaBatchInfo": {
                "batchSize": 4,
                "recordsInfo": {
                    "orders-1": {"firstRecordOffset": 3, "lastRecordOffset": 6}
                },
            },
            "timestamp": "2026-09-28T18:47:58.790Z",
            "version": "1.0",
        }
    ).encode()

    envelope, records = redrive.unwrap(sqs_shaped)

    assert records == []
    assert envelope["KafkaBatchInfo"]["recordsInfo"]["orders-1"]["firstRecordOffset"] == 3


def test_rebuilt_event_matches_the_esm_wire_format():
    """The reconstructed event must be consumable by the unmodified projection adapter.

    This is the payoff of the pure-core handler design: a redrive reuses the live projection
    instead of reimplementing it, so the two cannot drift.
    """
    _envelope, records = redrive.unwrap(_failure_object())

    event = redrive.rebuild_esm_event(records)

    assert event["eventSource"] == "aws:kafka"
    assert set(event["records"]) == {"orders-1"}
    assert len(event["records"]["orders-1"]) == 2

    # Feed it to the real adapter.
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
    from projection.adapter import records_from_esm_event

    parsed = records_from_esm_event(event)

    assert [r.offset for r in parsed] == [3, 4]
    assert parsed[0].dedup_key == "orders#1#3"
    assert parsed[1].payload()["id"] == "iso-42ade9-2"

"""Builders for realistic event source mapping payloads."""

from __future__ import annotations

import base64
import json
from typing import Any


def _b64(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def kafka_record(
    *,
    topic: str = "orders",
    partition: int = 0,
    offset: int = 0,
    timestamp_ms: int = 1_700_000_000_000,
    key: str | None = "order-1",
    payload: dict[str, Any] | None = None,
    raw_value: str | None = None,
    headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    """One record in ESM wire format: base64 value, headers as byte arrays."""
    if raw_value is None:
        body = payload if payload is not None else {
            "id": "order-1",
            "eventType": "OrderPlaced",
            "sourceTimestamp": timestamp_ms,
        }
        raw_value = json.dumps(body)

    record: dict[str, Any] = {
        "topic": topic,
        "partition": partition,
        "offset": offset,
        "timestamp": timestamp_ms,
        "timestampType": "CREATE_TIME",
        "value": _b64(raw_value),
    }
    if key is not None:
        record["key"] = _b64(key)
    if headers:
        record["headers"] = [
            {name: list(value.encode("utf-8"))} for name, value in headers.items()
        ]
    return record


def esm_event(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Group records under the `records` map exactly as the ESM delivers them."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        grouped.setdefault(f"{record['topic']}-{record['partition']}", []).append(record)

    return {
        "eventSource": "aws:kafka",
        "eventSourceArn": "arn:aws:kafka:us-east-1:111122223333:cluster/demo/abc-1",
        "bootstrapServers": "b-1.demo.kafka.us-east-1.amazonaws.com:9098",
        "records": grouped,
    }

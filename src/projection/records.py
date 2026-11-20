"""The neutral record shape.

This module is deliberately ignorant of where a record came from. An event source mapping
payload is one source; an archived object read back from S3 would be another. Keeping the
shape independent of the transport is what allows the same projection logic to serve a live
invocation and a replay driver without the projection being written twice.
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Record:
    """One Kafka record, decoded, with its position on the log."""

    topic: str
    partition: int
    offset: int
    timestamp_ms: int
    key: str | None
    value: str
    headers: dict[str, str] = field(default_factory=dict)

    @property
    def dedup_key(self) -> str:
        """Identity for at-least-once suppression.

        topic-partition-offset identifies a record within one incarnation of a topic, not
        universally. Offsets reset when a topic is rebuilt, compaction discards intermediate
        offsets, and a partition-count change moves an entity to a different partition. This key
        protects against redelivery of the same record by the same consumer group. Correctness
        across a rebuild needs a business key from the payload, with the offset as a tie-break.
        """
        return f"{self.topic}#{self.partition}#{self.offset}"

    @property
    def failure_identifier(self) -> dict[str, Any]:
        """The identifier shape Lambda expects for a Kafka partial batch response.

        For Kinesis, DynamoDB Streams and SQS, itemIdentifier is a plain string. For Kafka it is
        an object: {"partition": "<topic>-<partition>", "offset": <int>}.

        A partition or offset not present in the invoked event "is treated as failure and the
        entire batch is retried", so the wrong shape raises no error. It degrades per-record
        isolation back to whole-batch retry, which is what ReportBatchItemFailures exists to
        prevent.

        https://docs.aws.amazon.com/lambda/latest/dg/kafka-retry-configurations.html
        """
        return {"partition": f"{self.topic}-{self.partition}", "offset": self.offset}

    def payload(self) -> dict[str, Any]:
        """The message value parsed as JSON.

        Raises on malformed JSON rather than returning a sentinel. A record whose body cannot be
        read is a processing failure and should be reported as one.
        """
        return json.loads(self.value)


def _decode_b64(value: str | None) -> str | None:
    if value is None:
        return None
    return base64.b64decode(value).decode("utf-8")


def _decode_headers(raw: list[dict[str, list[int]]] | None) -> dict[str, str]:
    """Kafka headers arrive as a list of single-entry maps with byte-array values."""
    headers: dict[str, str] = {}
    for entry in raw or []:
        for name, byte_values in entry.items():
            headers[name] = bytes(byte_values).decode("utf-8")
    return headers

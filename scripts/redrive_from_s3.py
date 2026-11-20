#!/usr/bin/env python3
"""Reprocess failed records from the S3 on-failure destination.

SQS and SNS on-failure destinations receive metadata about the failed batch: topic, partition,
first and last offset, timestamps, but not the payload. A runbook that redrives records out of an
SQS DLQ has nothing to redrive, only coordinates to replay from Kafka.

An S3 destination receives the entire invocation record, which is what makes this script possible:
the failed payloads are in object storage and can be reprocessed whether or not the topic still
holds them.

The object has three layers of wrapping:

  1. The object is JSON with metadata keys plus a `payload` key.
  2. `payload` is a JSON **string**, not a nested object. It has to be parsed a second time.
  3. Inside, each record's `key` and `value` are **base64**, exactly as the event source mapping
     delivers them.

So the shape is:

    {
      "KafkaBatchInfo":  { "batchSize": 4, "recordsInfo": {"orders-1": {...}} },
      "requestContext":  { "requestId": "...", "functionArn": "..." },
      "responseContext": { "statusCode": 200 },
      "timestamp": "2026-09-28T18:47:58.790Z",
      "version": "1.0",
      "payload": "{\\"records\\":{\\"orders-1\\":[{...base64 key/value...}]}}"
    }

It reprocesses by reconstructing the event source mapping's event shape and invoking the
projection function with it, so there is no second implementation of the projection to drift.

The handler is idempotent, so redriving a record that has already been applied is a no-op rather
than a duplicate.

    ./redrive_from_s3.py --bucket BUCKET --function FUNCTION [--prefix P] [--apply]

Dry run by default: it prints what it would reprocess and stops. Nothing is invoked without
--apply.
"""

from __future__ import annotations

import argparse
import base64
import json
import sys
from typing import Any, Iterator


def _clients(region: str | None):
    """Build the AWS clients.

    boto3 is imported here rather than at module scope so that unwrap() and rebuild_esm_event()
    can be imported and tested without the SDK installed, which is the same arrangement the
    projection handler uses in src/projection/store.py.
    """
    try:
        import boto3
    except ImportError:  # pragma: no cover
        sys.exit("boto3 is required: pip install boto3")

    session = boto3.Session(region_name=region)
    return session.client("s3"), session.client("lambda")


def iter_objects(s3: Any, bucket: str, prefix: str) -> Iterator[str]:
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]


def unwrap(body: bytes) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Peel the three layers. Returns (metadata, records) with values decoded."""
    envelope = json.loads(body)

    raw_payload = envelope.get("payload")
    if raw_payload is None:
        # A metadata-only document, which is what an SQS or SNS destination produces. Not
        # redrivable: it carries coordinates and no payload.
        return envelope, []

    # Layer 2: payload is a STRING containing JSON.
    inner = json.loads(raw_payload) if isinstance(raw_payload, str) else raw_payload

    records: list[dict[str, Any]] = []
    for topic_partition, record_list in inner.get("records", {}).items():
        for record in record_list:
            records.append(
                {
                    "topic_partition": topic_partition,
                    "topic": record["topic"],
                    "partition": record["partition"],
                    "offset": record["offset"],
                    # Layer 3: key and value are base64.
                    "key": _b64_text(record.get("key")),
                    "value": _b64_text(record.get("value")),
                    "raw": record,
                }
            )
    return envelope, records


def _b64_text(value: str | None) -> str | None:
    if value is None:
        return None
    return base64.b64decode(value).decode("utf-8", errors="replace")


def rebuild_esm_event(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Reassemble the event source mapping's event shape from archived records.

    The archived records are already in ESM wire format, with base64 key and value and headers as
    byte arrays, so they can be handed back to the same handler untouched. Only the grouping map
    needs rebuilding.
    """
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        grouped.setdefault(record["topic_partition"], []).append(record["raw"])

    return {
        "eventSource": "aws:kafka",
        "eventSourceArn": "redrive",
        "bootstrapServers": "",
        "records": grouped,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bucket", required=True, help="Failure bucket (terraform output failure_bucket)")
    parser.add_argument("--function", required=True, help="Projection function to reprocess through")
    parser.add_argument("--prefix", default="", help="Limit to a prefix, e.g. aws/lambda/<esm-uuid>/2026/09/28/")
    parser.add_argument("--apply", action="store_true", help="Actually invoke. Without this, dry run.")
    parser.add_argument("--region", default=None)
    args = parser.parse_args()

    s3, lam = _clients(args.region)

    total_objects = total_records = invoked = skipped = 0

    for key in iter_objects(s3, args.bucket, args.prefix):
        body = s3.get_object(Bucket=args.bucket, Key=key)["Body"].read()
        envelope, records = unwrap(body)
        total_objects += 1

        if not records:
            batch = envelope.get("KafkaBatchInfo", {})
            print(f"  {key}\n    metadata only, nothing to redrive: {json.dumps(batch)[:120]}")
            skipped += 1
            continue

        total_records += len(records)
        print(f"  {key}")
        for record in records:
            print(
                f"    {record['topic']}[{record['partition']}]@{record['offset']} "
                f"key={record['key']} value={record['value'][:110]}"
            )

        if not args.apply:
            continue

        response = lam.invoke(
            FunctionName=args.function,
            Payload=json.dumps(rebuild_esm_event(records)).encode(),
        )
        result = json.loads(response["Payload"].read() or b"{}")
        failures = result.get("batchItemFailures", [])
        invoked += 1

        if failures:
            # Still failing, so the record is poison rather than the victim of a transient fault.
            # Reprocessing will not fix it; the payload needs inspection.
            print(f"    STILL FAILING: {json.dumps(failures)}")
        else:
            print("    reprocessed cleanly")

    print(
        f"\n{total_objects} object(s), {total_records} record(s), "
        f"{skipped} metadata-only, {invoked} invoked"
        + ("" if args.apply else "   [DRY RUN: pass --apply to reprocess]")
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

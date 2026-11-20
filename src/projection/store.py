"""DynamoDB access. The only module in the package that performs I/O.

Two tables per topic, and the split is deliberate:

  read model      the projection itself, one item per business key
  ledger          dedup marker + attempt counter, one item per topic-partition-offset, TTL'd

The ledger is the piece of the inbox pattern that does NOT go away when the transport becomes
managed. Kafka's log replaces the durable landing, consumer-group offsets replace progress
tracking, and the on-failure destination replaces failure quarantine. Nothing in the managed
pipeline records whether this handler already applied a given record. Delivery is at-least-once,
and on partial-batch failure retry resumes from the lowest reported failed offset, so records
after it that already succeeded are delivered again.
"""

from __future__ import annotations

import time
from typing import Any, Protocol


class Ledger(Protocol):
    """What the handler needs from the dedup/attempt store. Lets tests supply a fake."""

    def begin_attempt(self, dedup_key: str) -> tuple[int, bool]: ...
    def mark_processed(self, dedup_key: str) -> None: ...
    def park(self, dedup_key: str, reason: str) -> None: ...


class ReadModel(Protocol):
    def upsert(
        self, business_key: str, source_timestamp: int, attributes: dict[str, Any], removed: bool
    ) -> bool: ...


class DynamoLedger:
    """Dedup marker and attempt counter, in one item per record."""

    def __init__(self, table: Any, ttl_days: int = 7) -> None:
        self._table = table
        self._ttl_seconds = ttl_days * 86_400

    def begin_attempt(self, dedup_key: str) -> tuple[int, bool]:
        """Increment the attempt counter and report whether this record is already done.

        Two properties of this call matter.

        The counter is incremented before the work, not after. A counter written after the business
        logic does not record the failures that matter: if the projection write fails and rolls
        back, a same-transaction attempt counter rolls back with it, leaving the record looking
        untouched and eligible for retry indefinitely.

        It is also a separate write from the projection, not part of a TransactWriteItems with the
        read-model update. Retry state has to survive the rollback of the work it describes.

        ADD is atomic, so concurrent invocations for the same record cannot lose a count.
        """
        now = int(time.time())
        result = self._table.update_item(
            Key={"pk": dedup_key},
            UpdateExpression=(
                "ADD #attempts :one "
                "SET #last_attempt_at = :now, #expires_at = if_not_exists(#expires_at, :ttl)"
            ),
            ExpressionAttributeNames={
                "#attempts": "attempts",
                "#last_attempt_at": "last_attempt_at",
                "#expires_at": "expires_at",
            },
            ExpressionAttributeValues={
                ":one": 1,
                ":now": now,
                ":ttl": now + self._ttl_seconds,
            },
            ReturnValues="ALL_NEW",
        )
        item = result.get("Attributes", {})
        return int(item.get("attempts", 1)), bool(item.get("processed", False))

    def mark_processed(self, dedup_key: str) -> None:
        # `processed` is a DynamoDB reserved keyword and cannot appear literally in an
        # UpdateExpression; the call fails with a ValidationException naming it. `parked` below is
        # the same. Every attribute in this table is aliased through ExpressionAttributeNames
        # rather than only the ones currently known to collide, because the reserved list is long.
        #
        # A fake table records an UpdateExpression without validating it, so offline tests cannot
        # catch this class of error.
        self._table.update_item(
            Key={"pk": dedup_key},
            UpdateExpression="SET #processed = :true, #processed_at = :now",
            ExpressionAttributeNames={"#processed": "processed", "#processed_at": "processed_at"},
            ExpressionAttributeValues={":true": True, ":now": int(time.time())},
        )

    def park(self, dedup_key: str, reason: str) -> None:
        """Record that this record is being abandoned by business-rule decision.

        Note the tradeoff. Once the handler returns success for a record the offset advances, and
        the event source mapping will not send it to the on-failure destination. Parking therefore
        moves ownership of the record away from the managed path. With max_attempts at 0, the
        mapping's retry budget and its S3 destination remain the only exit.
        """
        self._table.update_item(
            Key={"pk": dedup_key},
            UpdateExpression="SET #parked = :true, #reason = :reason, #parked_at = :now",
            ExpressionAttributeNames={
                "#parked": "parked",
                "#reason": "parked_reason",
                "#parked_at": "parked_at",
            },
            ExpressionAttributeValues={
                ":true": True,
                ":reason": reason,
                ":now": int(time.time()),
            },
        )


class DynamoReadModel:
    """The projection, guarded by a conditional write."""

    def __init__(self, table: Any) -> None:
        self._table = table

    def upsert(
        self, business_key: str, source_timestamp: int, attributes: dict[str, Any], removed: bool
    ) -> bool:
        """Apply the projection only if it is not older than what is stored.

        This conditional expression replaces SELECT ... FOR UPDATE. It holds no lock and blocks no
        connection: concurrent writers both attempt, and the one carrying older state is rejected.
        The rejection is a no-op rather than an error. Returning False tells the handler the record
        was superseded, which must not be reported as a batch item failure, or the mapping retries
        a record that is correctly being ignored.

        It is also what makes replay safe: a replayed event older than stored state cannot regress
        it.
        """
        try:
            self._table.update_item(
                Key={"pk": business_key},
                UpdateExpression=(
                    "SET #source_ts = :ts, #attrs = :attrs, #removed = :removed, #updated_at = :now"
                ),
                ConditionExpression="attribute_not_exists(#source_ts) OR #source_ts < :ts",
                ExpressionAttributeNames={
                    "#source_ts": "source_ts",
                    "#attrs": "attrs",
                    "#removed": "removed",
                    "#updated_at": "updated_at",
                },
                ExpressionAttributeValues={
                    ":ts": source_timestamp,
                    ":attrs": attributes,
                    ":removed": removed,
                    ":now": int(time.time()),
                },
            )
            return True
        except Exception as exc:  # noqa: BLE001 - narrowed below
            if _is_conditional_check_failure(exc):
                return False
            raise


def _is_conditional_check_failure(exc: Exception) -> bool:
    """Identify a losing conditional write without importing botocore.

    Structural rather than importing botocore.exceptions, so this module can be imported and
    unit-tested without the AWS SDK installed.
    """
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        code = response.get("Error", {}).get("Code")
        return code == "ConditionalCheckFailedException"
    return type(exc).__name__ == "ConditionalCheckFailedException"


def tables_from_env(ledger_table: str, read_model_table: str, ttl_days: int = 7):
    """Build the live stores. boto3 is imported here so tests never need it."""
    import boto3  # noqa: PLC0415 - deliberately lazy

    dynamodb = boto3.resource("dynamodb")
    return (
        DynamoLedger(dynamodb.Table(ledger_table), ttl_days=ttl_days),
        DynamoReadModel(dynamodb.Table(read_model_table)),
    )

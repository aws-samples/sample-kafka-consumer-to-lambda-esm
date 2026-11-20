"""Lambda entry point: adapter -> core -> stores, and the partial batch response.

Decisions live in `core`; conditional writes live in `store`. This module sequences them per
record and translates the outcome into the response shape Lambda expects.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Any

from .adapter import records_from_esm_event
from .core import project
from .records import Record
from .store import Ledger, ReadModel, tables_from_env

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))


@dataclass
class Outcome:
    """Per-record result, for logging and for deciding the response."""

    applied: int = 0
    duplicate: int = 0
    superseded: int = 0
    parked: int = 0
    failed: int = 0


def process_records(
    records: list[Record],
    ledger: Ledger,
    read_model: ReadModel,
    max_attempts: int = 0,
) -> tuple[list[dict[str, Any]], Outcome]:
    """Apply a batch and report which records failed.

    max_attempts = 0, the default, means the handler never gives up on its own: the mapping's
    retry budget and its on-failure destination are the only exit for a poison record. See
    store.park for the tradeoff when setting it higher.

    Failures are collected rather than raised. An exception escaping this function fails the whole
    batch, which is the behaviour ReportBatchItemFailures exists to replace.
    """
    failures: list[dict[str, Any]] = []
    outcome = Outcome()

    for record in records:
        try:
            # Increment first, in its own write. See store.DynamoLedger.begin_attempt for why
            # the order and the separation both matter.
            attempts, already_processed = ledger.begin_attempt(record.dedup_key)

            if already_processed:
                # At-least-once delivery made this a duplicate. Not an error.
                outcome.duplicate += 1
                continue

            if max_attempts and attempts > max_attempts:
                ledger.park(record.dedup_key, f"exceeded max_attempts={max_attempts}")
                outcome.parked += 1
                logger.warning(
                    "parked record dedup_key=%s attempts=%d", record.dedup_key, attempts
                )
                continue

            decision = project(record, record.payload())

            applied = read_model.upsert(
                business_key=decision.business_key,
                source_timestamp=decision.source_timestamp,
                attributes=decision.attributes,
                removed=decision.removed,
            )

            if applied:
                outcome.applied += 1
            else:
                # The conditional write lost: stored state is newer. This is a no-op, not a
                # failure. Reporting it would retry a record that is correctly being ignored.
                outcome.superseded += 1

            ledger.mark_processed(record.dedup_key)

        except Exception:  # noqa: BLE001 - one bad record must not fail its batch
            outcome.failed += 1
            logger.exception(
                "record failed topic=%s partition=%d offset=%d",
                record.topic,
                record.partition,
                record.offset,
            )
            failures.append({"itemIdentifier": record.failure_identifier})

    return failures, outcome


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    ledger, read_model = tables_from_env(
        ledger_table=os.environ["LEDGER_TABLE"],
        read_model_table=os.environ["READ_MODEL_TABLE"],
        ttl_days=int(os.environ.get("LEDGER_TTL_DAYS", "7")),
    )

    records = records_from_esm_event(event)
    failures, outcome = process_records(
        records,
        ledger=ledger,
        read_model=read_model,
        max_attempts=int(os.environ.get("MAX_ATTEMPTS", "0")),
    )

    logger.info(
        "batch complete %s",
        json.dumps(
            {
                "received": len(records),
                "applied": outcome.applied,
                "duplicate": outcome.duplicate,
                "superseded": outcome.superseded,
                "parked": outcome.parked,
                "failed": outcome.failed,
            }
        ),
    )

    return {"batchItemFailures": failures}

"""Store tests: the conditional expressions, without AWS.

These assert on the DynamoDB expressions themselves. The two properties they encode, an
increment-before-work in a separate write and a precedence-guarded conditional upsert, are
invisible to the handler-level tests.
"""

from __future__ import annotations

from projection.store import DynamoLedger, DynamoReadModel

from .fakes import RecordingTable, RejectingTable


def test_begin_attempt_uses_an_atomic_add_and_returns_state():
    table = RecordingTable(returns={"Attributes": {"attempts": 3, "processed": False}})
    ledger = DynamoLedger(table, ttl_days=7)

    attempts, processed = ledger.begin_attempt("orders#0#1")

    (call,) = table.calls
    assert "ADD #attempts :one" in call["UpdateExpression"], "must be atomic, not read-modify-write"
    assert call["ReturnValues"] == "ALL_NEW", "one round trip for count and dedup state"
    assert call["Key"] == {"pk": "orders#0#1"}
    assert attempts == 3 and processed is False


def test_begin_attempt_sets_a_ttl_only_once():
    """if_not_exists keeps the expiry anchored to first sight, not to the latest retry."""
    table = RecordingTable(returns={"Attributes": {"attempts": 1}})

    DynamoLedger(table, ttl_days=7).begin_attempt("orders#0#1")

    (call,) = table.calls
    assert "#expires_at = if_not_exists(#expires_at, :ttl)" in call["UpdateExpression"]
    assert call["ExpressionAttributeValues"][":ttl"] > call["ExpressionAttributeValues"][":now"]


def test_ledger_writes_are_separate_calls_never_a_transaction():
    """Bundling the counter with the projection write recreates the original defect.

    The incident this pattern answers was a rollback erasing the same-transaction write that
    marked a record failed. Retry state has to live outside the transaction whose rollback it is
    meant to survive, so the ledger exposes plain UpdateItem calls and no transactional API.
    """
    table = RecordingTable(returns={"Attributes": {"attempts": 1}})
    ledger = DynamoLedger(table)

    ledger.begin_attempt("orders#0#1")
    ledger.mark_processed("orders#0#1")

    assert len(table.calls) == 2
    assert not hasattr(ledger, "transact_write")


def test_upsert_is_guarded_by_a_precedence_condition():
    table = RecordingTable()

    applied = DynamoReadModel(table).upsert(
        business_key="o1", source_timestamp=500, attributes={"total": 1}, removed=False
    )

    (call,) = table.calls
    assert call["ConditionExpression"] == "attribute_not_exists(#source_ts) OR #source_ts < :ts"
    assert call["ExpressionAttributeValues"][":ts"] == 500
    assert applied is True


def test_losing_the_condition_returns_false_rather_than_raising():
    """A superseded record is a no-op, not an error.

    This is the replacement for SELECT ... FOR UPDATE: no lock is held and nothing blocks. The
    writer carrying older state is rejected, and the handler must see that as "already newer"
    rather than as a failure to retry.
    """
    applied = DynamoReadModel(RejectingTable()).upsert(
        business_key="o1", source_timestamp=100, attributes={}, removed=False
    )

    assert applied is False


def test_other_errors_still_propagate():
    class Broken:
        def update_item(self, **kwargs):
            raise RuntimeError("throughput exceeded")

    try:
        DynamoReadModel(Broken()).upsert(
            business_key="o1", source_timestamp=1, attributes={}, removed=False
        )
    except RuntimeError as exc:
        assert "throughput" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("a real error must not be swallowed as a precedence miss")


def test_every_update_expression_aliases_its_attribute_names():
    """Regression guard for a bug the offline fakes could not see.

    `processed` is a DynamoDB reserved keyword, so an UpdateExpression containing it literally is
    rejected with a ValidationException at runtime. A fake table records the expression without
    validating it, so the first sign of trouble was a record retried four times in a live
    account.

    Rather than special-case the keywords currently known to collide, every attribute reference
    in every expression is aliased. This test enforces that: any bare attribute name in an
    expression fails it.
    """
    table = RecordingTable(returns={"Attributes": {"attempts": 1}})
    ledger = DynamoLedger(table)

    ledger.begin_attempt("k")
    ledger.mark_processed("k")
    ledger.park("k", "reason")
    DynamoReadModel(table).upsert(
        business_key="o1", source_timestamp=1, attributes={}, removed=False
    )

    for call in table.calls:
        expression = call["UpdateExpression"] + call.get("ConditionExpression", "")
        names = call.get("ExpressionAttributeNames", {})

        # Everything that is not a placeholder, a function, a keyword or punctuation must be a
        # `#alias`: i.e. no bare attribute names survive.
        import re

        bare = re.findall(r"(?<![:#\w])([a-z_][a-z0-9_]*)(?=\s*[=,)]|\s+:)", expression)
        allowed = {"attribute_not_exists", "if_not_exists"}
        assert not (set(bare) - allowed), f"unaliased attribute(s) {set(bare) - allowed} in {expression!r}"
        assert all(alias in expression for alias in names), "declared an alias it does not use"

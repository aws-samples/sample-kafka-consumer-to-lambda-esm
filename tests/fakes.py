"""Test doubles. No AWS, no boto3, no network."""

from __future__ import annotations

from typing import Any


class ConditionalCheckFailed(Exception):
    """Shaped like a botocore ClientError so store._is_conditional_check_failure sees it."""

    def __init__(self) -> None:
        super().__init__("conditional check failed")
        self.response = {"Error": {"Code": "ConditionalCheckFailedException"}}


class FakeLedger:
    def __init__(self) -> None:
        self.attempts: dict[str, int] = {}
        self.processed: set[str] = set()
        self.parked: dict[str, str] = {}
        self.calls: list[str] = []

    def begin_attempt(self, dedup_key: str) -> tuple[int, bool]:
        self.calls.append(f"begin_attempt:{dedup_key}")
        self.attempts[dedup_key] = self.attempts.get(dedup_key, 0) + 1
        return self.attempts[dedup_key], dedup_key in self.processed

    def mark_processed(self, dedup_key: str) -> None:
        self.calls.append(f"mark_processed:{dedup_key}")
        self.processed.add(dedup_key)

    def park(self, dedup_key: str, reason: str) -> None:
        self.calls.append(f"park:{dedup_key}")
        self.parked[dedup_key] = reason


class FakeReadModel:
    """Applies the same precedence rule the conditional write enforces server-side."""

    def __init__(self) -> None:
        self.items: dict[str, dict[str, Any]] = {}
        self.calls: list[str] = []

    def upsert(
        self, business_key: str, source_timestamp: int, attributes: dict[str, Any], removed: bool
    ) -> bool:
        self.calls.append(f"upsert:{business_key}")
        stored = self.items.get(business_key)
        if stored is not None and source_timestamp <= stored["source_ts"]:
            return False
        self.items[business_key] = {
            "source_ts": source_timestamp,
            "attrs": attributes,
            "removed": removed,
        }
        return True


class ExplodingReadModel:
    """Fails for chosen business keys, to exercise the failure path."""

    def __init__(self, fail_keys: set[str]) -> None:
        self.fail_keys = fail_keys
        self.applied: list[str] = []

    def upsert(
        self, business_key: str, source_timestamp: int, attributes: dict[str, Any], removed: bool
    ) -> bool:
        if business_key in self.fail_keys:
            raise RuntimeError(f"malformed field in {business_key}")
        self.applied.append(business_key)
        return True


class RecordingTable:
    """Captures update_item calls so the ledger's expressions can be asserted on."""

    def __init__(self, returns: dict[str, Any] | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self._returns = returns or {}

    def update_item(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        return self._returns


class RejectingTable:
    """Raises a conditional-check failure, as DynamoDB does when a precedence guard loses."""

    def update_item(self, **kwargs: Any) -> dict[str, Any]:
        raise ConditionalCheckFailed()

"""Test producer: a VPC-attached Lambda.

The brokers sit in private subnets with no NAT gateway, so nothing outside the VPC can reach them.
The alternatives to a VPC-attached Lambda are a bastion host or a VPC-enabled CloudShell session,
both of which add setup a reader has to reproduce. This runs from a laptop with only AWS
credentials:

    aws lambda invoke --function-name <prefix>-producer \\
        --cli-binary-format raw-in-base64-out \\
        --payload '{"scenario": "header_only"}' /dev/stdout

The projection function is not VPC-attached, because it reaches DynamoDB over the AWS network and
never contacts a broker; the event source mapping's poller does that. The producer is an ordinary
Kafka client, so it has to run where the brokers are.

This module needs a Kafka client and the MSK IAM signer, neither of which the runtime provides, so
this function has a build step and the projection does not. Scenario payloads live in scenarios.py
so they can be tested without those dependencies.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from kafka import KafkaProducer

from scenarios import SCENARIOS

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
BOOTSTRAP = os.environ.get("BOOTSTRAP_BROKERS", "")
DEFAULT_TOPIC = os.environ.get("DEFAULT_TOPIC", "orders")
DEFAULT_EVENT_TYPE = os.environ.get("DEFAULT_EVENT_TYPE", "OrderPlaced")


class _TokenProvider:
    """MSK IAM auth: the token is derived from this function's execution role.

    No SASL password exists in this repository: the execution role is the Kafka client identity,
    so there is no secret to store or rotate.
    """

    def token(self) -> str:
        # generate_auth_token returns (token, expiry_ms). kafka-python's OAUTHBEARER provider
        # contract expects the token string only. Returning the tuple fails authentication, and
        # the symptom is a bare NoBrokersAvailable that says nothing about credentials.
        token, _expiry_ms = MSKAuthTokenProvider.generate_auth_token(REGION)
        return token


def _producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP.split(","),
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=_TokenProvider(),
        value_serializer=lambda v: v if isinstance(v, bytes) else json.dumps(v).encode(),
        key_serializer=lambda k: None if k is None else str(k).encode(),
        # The precedence and ordering scenarios depend on records landing in send order. More than
        # one in-flight request per connection would let a retry reorder them.
        max_in_flight_requests_per_connection=1,
        acks="all",
        retries=3,
        # The default api-version probe window is too short for a cold start that also has to
        # complete a SASL/IAM handshake against two brokers.
        api_version_auto_timeout_ms=30_000,
        request_timeout_ms=40_000,
    )


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    scenario = event.get("scenario", "matching")
    topic = event.get("topic", DEFAULT_TOPIC)
    event_type = event.get("event_type", DEFAULT_EVENT_TYPE)

    if scenario not in SCENARIOS:
        return {"error": f"unknown scenario {scenario!r}", "available": sorted(SCENARIOS)}

    records, expectation = SCENARIOS[scenario](topic, event_type)

    producer = _producer()
    produced: list[dict[str, Any]] = []
    try:
        for key, value, headers in records:
            metadata = producer.send(topic, key=key, value=value, headers=headers).get(timeout=30)
            produced.append(
                {
                    "topic": metadata.topic,
                    "partition": metadata.partition,
                    "offset": metadata.offset,
                    "key": key,
                }
            )
        producer.flush(timeout=30)
    finally:
        producer.close(timeout=10)

    logger.info("produced %d records for scenario=%s", len(produced), scenario)

    return {
        "scenario": scenario,
        "topic": topic,
        "produced": produced,
        "what_to_expect": expectation,
    }

# Consuming Amazon MSK into Amazon DynamoDB read models with AWS Lambda

A deployable sample: one Lambda event source mapping per Kafka topic, one function per topic, one
Amazon DynamoDB read model per topic. Failed records are captured whole in Amazon S3. Consumer lag is alarmed
through Amazon CloudWatch.

> **Not for production use.** This repository is sample code for learning and evaluation. It has not
> had the hardening a production deployment needs. See [SECURITY.md](SECURITY.md) for the known gaps
> and the changes to make before using it with real data.

This sample assumes familiarity with Kafka consumer groups and offsets.

Several event source mapping behaviours are commonly misconfigured, and two of them fail without
producing an error. This sample makes them observable. See [What this proves](#what-this-proves).

![Reference architecture: producers to Amazon MSK, one Lambda event source mapping per topic, each projecting into its own DynamoDB read model with a dedup and attempt ledger, failures captured whole in Amazon S3, and consumer lag alarmed through Amazon CloudWatch to Amazon SNS](docs/architecture.png)

Scope is stateless projection: each event is processed on its own, and all durable state lives in
the datastore. Stateful projection (aggregation, windowing, stream joins) belongs on Amazon Managed
Service for Apache Flink and is out of scope.

<details>
<summary><b>What this replaces</b></summary>

![The hand-built design: a Java service on application instances running a Kafka poll loop, an inbox writer, a SELECT FOR UPDATE SKIP LOCKED claim loop, an orphan sweeper with cleanup and retry jobs, and projection handlers, all contending on inbox and read-model tables in one relational store](docs/architecture-before.png)

Every box in that diagram is application code: a poll loop, an inbox writer, a claim loop, a
sweeper, cleanup and retry jobs, and the projection itself. The inbox table holds four kinds of
state at once. Queue state (what is pending), claim state (who is working on it), retry state (how
many attempts), and intake state (every event received, including those never processed).

Three of those four have purpose-built answers. The fourth, idempotency, remains application code in
this pattern too. See [What stays your responsibility](#what-stays-your-responsibility).

The diagram shows a well-built version of that design: one table per topic, `SKIP LOCKED` claiming,
precedence gating on a source timestamp. The comparison is against a correct hand-built consumer
that still has to be operated, not against a broken one.

Both diagrams are pages of [`docs/architecture.drawio`](docs/architecture.drawio).

</details>

## Prerequisites

This repository deploys the pattern, not the Kafka cluster. Bring your own MSK cluster and topics.
The interface is a set of variables rather than a remote-state lookup, so pointing it at an existing
cluster is the normal case.

| Prerequisite | Why |
| --- | --- |
| MSK cluster with AWS Identity and Access Management (IAM) authentication | Provisioned and Serverless both work. The Lambda execution role is the Kafka client identity, so there is no secret to store or rotate |
| One Kafka topic per projection | Create each with **2 or more partitions**. Event pollers are capped at the partition count, so a single-partition topic cannot demonstrate concurrent processing |
| Cluster security group allowing 9098 from itself | The event source mapping creates network interfaces in the cluster's subnets and security group, not the function's |
| S3 gateway endpoint in the cluster VPC | Required for on-failure delivery. Gateway endpoints have no hourly or data charge |
| Topic retention covering your recovery window | Retention bounds how far a replay can reach. See [Replay](#replay) |

## Deploy

```bash
cp terraform/prereq.auto.tfvars.example terraform/prereq.auto.tfvars
# Fill in six values from your cluster: ARN, IAM bootstrap brokers, VPC id,
# private subnet ids, cluster security group id, and your topic names.

cd terraform
terraform init
terraform apply
```

The apply takes about 12 minutes. Event source mappings account for nearly all of it, at roughly 11
minutes each, created in parallel. A mapping that reports `Creating` for ten minutes is behaving
normally.

To produce events, use the test producer. It runs inside the VPC, so no bastion host or VPN is
needed:

```bash
./scripts/run_demo.sh matching        # one event, one read-model item
./scripts/run_demo.sh header_only     # filtered out; the function is never invoked
./scripts/run_demo.sh isolation       # 5 processable records and 1 poison record, mid-batch
./scripts/run_demo.sh stale           # an out-of-order event, superseded by precedence
./scripts/run_demo.sh ordering        # 10 records, poison record at position 4
```

Each run prints what to expect and where to look for it. `scripts/redrive_from_s3.py` reprocesses
failed records from the S3 destination.

After a run, check three places:

- **Ledger and read-model tables** in Amazon DynamoDB: one item per processed record, with an
  attempt count in the ledger.
- **`FilteredOutEventCount`** in Amazon CloudWatch, dimensioned by `EventSourceMappingUUID`: it
  rises for the `header_only` scenario and nothing else shows the drop.
- **The failures bucket** in Amazon S3 (`terraform output failure_bucket`): the `isolation` and
  `ordering` scenarios leave the poison record here, payload included.

Plan for the apply time above plus the time to run and check each scenario.

## What this proves

Each item below is demonstrated by the test producer.

**1. Filter criteria match the message value, never Kafka headers.**

The filter pattern is `{"value": {"eventType": [...]}}`. There is no header equivalent. An event
whose type is set only in a Kafka header is dropped before the function runs, with no error and no
log entry. Offsets are committed for filtered records, so no backlog accumulates to reveal the
problem later. `FilteredOutEventCount` is the only signal.

**2. Only S3 receives the full record on failure.**

Amazon SQS and Amazon SNS on-failure destinations receive metadata about the failed batch: topic, partition, first
and last offset, timestamps. S3 receives the metadata and the records. A runbook that redrives
records out of an SQS DLQ cannot work, because the records are not there.

**3. Per-record isolation is off by default.**

Whole-batch retry is the default. Isolation requires `ReportBatchItemFailures` on the mapping and a
`batchItemFailures` response from the function. For Kafka sources that identifier is an object,
`{"partition": "<topic>-<partition>", "offset": <int>}`, not the plain string used by Kinesis,
DynamoDB Streams and SQS. An identifier Lambda cannot match against the invoked event is treated as
a failure, and the whole batch is retried.

**4. The networking requirement changes with the scaling mode, and misconfiguration is silent.**

In on-demand mode the mapping invokes the function over a path through your VPC, which requires a
NAT gateway or PrivateLink endpoints for Lambda and STS. In provisioned mode Lambda handles that
connection and neither is required. This sample uses provisioned mode.

Two workarounds that look correct do not help. The function always runs in a Lambda-managed VPC, so
moving it out of yours changes nothing. S3 and DynamoDB gateway endpoints serve the handler's own
egress, not the invoke path. Misconfigured on-demand mode polls successfully and never invokes.

**5. Partial-batch retry redelivers to the end of the failing batch, not the end of the partition.**

Run `./scripts/run_demo.sh ordering` and read the ledger table. Records before the failure show
`attempts=1`. The poison record and the records after it *within the same batch* show `attempts=4`.
Records that arrived in a later batch show `attempts=1`. A poison record's blast radius is bounded
by `batch_size`, and those repeats are why the dedup ledger is required rather than optional.

One further behaviour is documented rather than demonstrated, since reproducing it means
deliberately misconfiguring the stack:

- **The two CloudWatch metric families use different dimensions.** `OffsetLag` is dimensioned by
  `FunctionName`. Every opt-in event source mapping metric, including `FilteredOutEventCount`,
  `OnFailureDestinationDeliveredEventCount` and `MaxOffsetLag`, is dimensioned by
  `EventSourceMappingUUID`. An alarm written against the wrong dimension matches nothing and stays
  in `INSUFFICIENT_DATA`, which is indistinguishable from a healthy system.

## What stays your responsibility

The pattern replaces polling, claiming, retrying, dead-lettering, housekeeping and lag monitoring.
It does not replace:

**Idempotency.** Delivery is at-least-once. On partial-batch failure, retry resumes from the lowest
reported failed offset, so records after it that already succeeded are delivered again. Each topic
has a dedup and attempt-ledger table for this.

**Ordering by business key.** One poller per partition preserves order within that partition. Two
invocations can touch the same entity only if its events land on different partitions, so aligning
the partition key with the business key is the decision that determines whether concurrent writes to
one entity are possible. No service makes that choice for you.

**No-op detection and out-of-order protection.** Precedence gating on a source timestamp is
application logic. This sample implements it as a DynamoDB conditional write: apply only when the
stored timestamp is older than the incoming one. A record rejected by that condition is a no-op and
must not be reported as a batch item failure, or the mapping will retry it against state that stays
newer.

## Replay

Kafka retention is the replay window. A replay is always a new mapping, for three reasons:

- `starting_position`, `consumer_group_id` and `topics` are create-only.
- Lambda ignores `starting_position` when the consumer group already has committed offsets.
- AWS allows one mapping per (cluster, function, topic) triple, so a replay needs its own function
  rather than only its own table.

Blue-green replay ships behind `enable_replay` (default `false`): a second read-model table, a
separate dedup ledger, a second function running the same handler against them, and a replay mapping
with a fresh consumer group. Verify the new table, switch readers, then set the toggle back to
`false` to delete the replay mapping and stop its poller charge.

Two constraints shape that design:

**It rebuilds from `TRIM_HORIZON`.** A full rebuild is the simplest replay to reason about. For a
windowed replay, `AT_TIMESTAMP` with a `starting_position_timestamp` works on an MSK source: a new
mapping with a consumer group that has never consumed reads only the records at or after that time.
Lambda ignores the starting position once the group has committed offsets, so a windowed replay
needs a fresh group.

**The replay uses a separate dedup ledger.** Sharing the live ledger means every record the live path
already processed is skipped as a duplicate, and the rebuild produces almost nothing. A fresh table
has no duplicates to suppress. The replay still needs its own ledger, because at-least-once delivery
applies to the replay mapping as well.

Not built here: a raw S3 archive with a direct-invoke driver, which is the option that survives not
controlling the topic's retention; re-publishing to a dedicated replay topic; Athena over an archive
for record-level selectivity; and in-place replay behind strict precedence gating.

## Cost

This repository's resources are close to free when idle. Lambda and DynamoDB are usage-billed, S3
holds only failed records, and provisioned mode adds one Event Poller Unit at about USD 0.185/hour,
roughly USD 4.44/day.

The cluster is the expensive part, and this repository does not create it. Two `kafka.t3.small`
brokers, the cheapest Standard pair and sufficient for this sample, cost about USD 2/day or USD
60/month. A `kafka.m5.large` pair is about USD 10/day. MSK Serverless bills a USD 0.75 cluster-hour
floor regardless of traffic, about USD 18/day.

Rates are US East (N. Virginia). Check current pricing before relying on these figures.

## Clean up

```bash
cd terraform
terraform destroy
```

`terraform destroy` removes the DynamoDB read-model and ledger tables, the CloudWatch alarms, the
Amazon SNS topic, the functions, the event source mappings and the roles. The AWS KMS key is
scheduled for deletion after its waiting period (7 days by default) and is billed until then.

The failures bucket is created with `force_destroy = true`, so `destroy` empties and deletes it, and
**the failed records in it are lost permanently**. Copy out anything you need first, or reprocess it
with `scripts/redrive_from_s3.py`.

Destroy this stack before the cluster. The event source mapping's network interfaces live in the
cluster's subnets and block subnet and VPC deletion while the mapping exists.

Expect a delay even in the correct order. Lambda reclaims VPC ENIs asynchronously, so the producer's
interfaces outlive the function by a few minutes. A cluster deletion that fails on a dependent
network interface usually succeeds on retry.

## Security

Accepted trade-offs and production hardening steps are listed in [SECURITY.md](SECURITY.md).

One execution role per topic. A shared role would be shorter, and would also let the orders function
write the shipments read model. Each role grants:

- `kafka-cluster:Connect` and `DescribeCluster` on the cluster; `DescribeTopic` and `ReadData`
  scoped to that topic's ARN; `AlterGroup` and `DescribeGroup` on the consumer group.
- `kafka:DescribeClusterV2` and `GetBootstrapBrokers`, so the mapping can resolve brokers.
- `dynamodb:GetItem`, `PutItem` and `UpdateItem` on that topic's two tables only.
- `s3:PutObject` on the failure bucket's objects and `s3:ListBucket` on the bucket.

Two of those need explaining.

**`s3:ListBucket` is required, not optional.** `CreateEventSourceMapping` validates the on-failure
destination before the mapping becomes active and fails with *"The function execution role does not
have permissions to call ListBucket on S3"* when only `PutObject` is granted. The failure arrives
several minutes into mapping creation, after the plan has looked clean.

**Amazon EC2 network-interface permissions appear on a function that is not VPC-attached.** The projection
function has no `vpc_config`. The mapping's poller runs in the cluster's subnets and security group
under this execution role, and those EC2 actions do not support resource-level permissions, so the
resource is `*`.

IAM authentication means no Kafka credential exists in this repository. The execution role is the
client identity.

The failure bucket holds full message payloads and inherits the sensitivity of the events flowing
through the topic. Public access is blocked, encryption is enabled, and a lifecycle rule bounds how
long payloads persist.

The DynamoDB tables are encrypted with a customer-managed AWS KMS key whose policy permits
cryptographic operations only when the call arrives through DynamoDB, so an IAM grant of
`kms:Decrypt` cannot be used to read ciphertext directly. Point-in-time recovery is enabled: the
tables are rebuildable from the topic, but only while the records remain within its retention
window, and that window is often set by whoever owns the topic.

Network-interface permissions are split by whether the action can be scoped. The four `ec2:Describe`
actions keep `"*"` because they do not support resource-level permissions and are read-only.
`CreateNetworkInterface` and `DeleteNetworkInterface` are scoped to the network interfaces, subnets
and security group in use.

Where this still differs from production: the failure bucket uses SSE-S3 rather than a
customer-managed key; CloudWatch log groups use default encryption; AWS X-Ray tracing is off; and there
is no Lambda-level dead-letter configuration, so the on-failure destination covers failed records
but not invocation-level failures.

### Submitting this repository for a content scan

Build the archive with `./scripts/package_for_review.sh`, which uses `git archive` so the contents
are exactly the tracked files, and fails if a build artifact, state file, populated tfvars file or
virtualenv appears in the output.

Do not zip the working directory. `terraform/.terraform/producer-build/` holds the producer's
pip-installed dependencies, 536 third-party Python files from setuptools, click, urllib3, dateutil,
jmespath and six. They are gitignored and are not part of this repository, but an archive built
from the working directory includes them, and a scanner then reports their subprocess calls and
`exec()` usage against this project. One such scan returned 399 findings, of which 15 were in
tracked files.

Findings are scanned with [Automated Security Helper](https://github.com/awslabs/automated-security-helper).
`.ash.yaml` records every suppression with a reason and a category.

## Layout

```
src/projection/     the handler. A pure core over a list of records, plus a thin event source
                    mapping adapter. The pure core is what lets an archived record be replayed
                    through the same logic.
src/producer/       the test producer and its demo scenarios
terraform/          mappings, functions, tables, failure bucket, lag alarm, replay path
tests/              offline tests; no AWS calls
scripts/            run_demo.sh, redrive_from_s3.py, package_for_review.sh
docs/               architecture.drawio (2 pages) and the exported PNGs
```

# Security Policy

## Disclaimer

This repository is sample code for learning and evaluation. It is **not intended for production use**
without the additional hardening described below. The trade-offs listed under Known Security
Considerations were made deliberately to keep the sample readable and cheap to run; they are not
recommendations for a production deployment.

## Reporting Vulnerabilities

If you discover a potential security issue in this project, notify AWS Security through the
[vulnerability reporting page](https://aws.amazon.com/security/vulnerability-reporting/) or email
aws-security@amazon.com. Do **not** create a public GitHub issue.

Please include a description of the vulnerability, steps to reproduce, and any potential impact. You
will receive acknowledgement of your report within 48 hours.

## AWS Services Used

- Amazon Managed Streaming for Apache Kafka (Amazon MSK): the event source. Consumed, not created, by
  this repository; you bring an existing cluster with IAM authentication.
- AWS Lambda: one projection function per topic, a VPC-attached test producer, and (optionally) one
  replay function per topic, each with an event source mapping.
- Amazon DynamoDB: one read-model table and one dedup/attempt-ledger table per topic, plus replay
  variants when `enable_replay` is set.
- AWS Key Management Service (AWS KMS): one customer-managed key encrypting all DynamoDB tables.
- Amazon Simple Storage Service (Amazon S3): the on-failure destination. Holds full message payloads
  for failed records.
- Amazon CloudWatch: log groups, consumer-lag and on-failure alarms.
- Amazon Simple Notification Service (Amazon SNS): alarm notifications.
- Amazon Elastic Compute Cloud (Amazon EC2): network interfaces created by the event source mapping
  and the producer in the cluster's subnets.
- AWS Identity and Access Management (IAM): per-topic execution roles; also the Kafka client
  identity through MSK IAM authentication.

## Prerequisites and Permissions

- An AWS account and an identity permitted to create IAM roles and policies, Lambda functions and
  event source mappings, DynamoDB tables, S3 buckets and bucket policies, KMS keys and aliases,
  CloudWatch log groups and alarms, SNS topics, and EC2 network interfaces in the cluster VPC.
- An existing Amazon MSK cluster (provisioned or serverless) with IAM authentication enabled, at
  least one topic per projection with two or more partitions, a security group that permits port
  9098 from itself, and an S3 gateway endpoint in the cluster VPC.
- Terraform >= 1.9, AWS provider >= 6.44.0 < 7, archive provider >= 2.6 < 3, Python 3 and pip on the
  machine running `terraform apply` (the producer package is built at plan time).

## Known Security Considerations

These were reviewed and accepted for a non-production sample. Each row describes the trade-off and
the production change.

| Item | Category | Rationale and production change |
|------|----------|----------------------------------|
| Failures bucket uses SSE-S3 (AES256), not a customer-managed key | Security Debt | The bucket holds full message payloads. SSE-S3 encrypts at rest but offers no CloudTrail audit of decryption and no key-policy revocation. Production: SSE-KMS with a CMK, `bucket_key_enabled = true`, and `kms:GenerateDataKey` / `kms:Decrypt` on that key in `failure_destination`, because the event source mapping writes under the function's execution role. |
| Account root is the KMS key administrator | Security Debt | The console-default pattern. Any IAM principal in the account holding key-management actions can administer the key. Production: replace the root principal with your deployment role ARN and narrow `kms:Create*` to `kms:CreateAlias`. |
| Lambda environment variables use the Lambda-managed key, not a CMK | Security Debt | Variables hold table names, broker endpoints, topic names and a log level; no secrets. Production: set `kms_key_arn` on each function if you add anything sensitive, or read secrets from AWS Secrets Manager at init instead. |
| CloudWatch log groups use default encryption, not a CMK | Security Debt | Logs contain counts and `topic#partition#offset` keys only. Production: a CMK whose policy grants `logs.<region>.amazonaws.com` the Encrypt/Decrypt/GenerateDataKey family scoped by `kms:EncryptionContext:aws:logs:arn`, referenced as `kms_key_id` on each log group. |
| No S3 server access logging on the failures bucket | Security Debt | Needs a second bucket. Production: `aws_s3_bucket_logging` to a dedicated logs bucket, or CloudTrail S3 data events for this bucket. |
| Lambda functions are not code-signed | Security Debt | The sample is built from source on your machine. Production: an AWS Signer profile and `aws_lambda_code_signing_config` with `untrusted_artifact_on_deployment = "Enforce"`. |
| AWS X-Ray tracing is off | Non-blocking | Observability, not a security control. Enable `tracing_config { mode = "Active" }` and grant `xray:PutTraceSegments` / `xray:PutTelemetryRecords` when wanted. |
| Deletion settings favour clean teardown | Non-blocking | `force_destroy = true` on the bucket, `prevent_destroy = false` on tables, 7-day KMS deletion window. Production: `prevent_destroy = true` on tables, `force_destroy = false`, and a 30-day key deletion window; a deleted key makes every item encrypted under it permanently unreadable. |

## Production Hardening Recommendations

Before using this code with real data:

- **IAM.** Replace the KMS key policy's root administrator with a named deployment role and narrow
  `kms:Create*` to `kms:CreateAlias`. Keep the consumer-group grants scoped to the exact group ARNs.
- **Encryption.** Move the failures bucket to SSE-KMS; put a CMK on the three log groups; set
  `kms_key_arn` on the functions if environment variables ever carry anything sensitive.
- **Logging and audit.** Enable S3 access logging or CloudTrail data events on the failures bucket.
  Review `log_retention_days` against your retention policy (default 365).
- **Supply chain.** Add code signing for the Lambda packages and run `pip-audit` (or an OSV query) on
  `src/producer/requirements.txt` in CI.
- **Data lifecycle.** Set `dlq_expiration_days` to the shortest period your redrive process needs;
  the bucket holds complete records.
- **Deletion protection.** Enable `prevent_destroy` on the tables, disable `force_destroy` on the
  bucket, and raise the KMS deletion window to 30 days.
- **Scope.** Payload sensitivity flows through to DynamoDB, S3 and (if you add it) logs. Classify the
  topic's data before deploying, and keep the per-topic role model when adding topics.

## Resource Cleanup

1. From `terraform/`, run `terraform destroy`. Destroy **this stack before the MSK cluster**: the
   event source mapping's network interfaces live in the cluster's subnets and block subnet and VPC
   deletion while the mapping exists (the `teardown_order` output repeats this on every apply).
2. The failures bucket has `force_destroy = true`, so it is emptied and removed with the stack.
3. The KMS key is scheduled for deletion with a 7-day window (`kms_key_deletion_window_days`); it is
   billed until then and can be cancelled within the window.
4. Lambda reclaims the producer's VPC network interfaces asynchronously. If cluster deletion later
   fails on a dependent network interface, retry after a few minutes.

## Dependencies

| Dependency | Version | Notes |
|------------|---------|-------|
| kafka-python-ng | 2.2.3 | Producer only. Pure Python. No known advisories (OSV, 2026-10-28). |
| aws-msk-iam-sasl-signer-python | 1.0.2 | Producer only. Generates the SASL/IAM token from the execution role. 1.0.2 uses `importlib.metadata`, removing the setuptools / `pkg_resources` requirement. No known advisories. |
| boto3 / botocore | Lambda runtime | Provided by the Python 3.12 runtime; pruned from the producer package at build time. |
| hashicorp/aws (Terraform) | >= 6.44.0, < 7.0.0 | 6.44 is the floor for `provisioned_poller_config`. |
| hashicorp/archive (Terraform) | >= 2.6.0, < 3.0.0 | Packages the projection from `src/`. |
| pytest | >= 8.0 (dev extra) | Offline tests only; not deployed. |

# The on-failure destination.
#
# S3, not SQS, and the reason is the whole of correction 2: an SQS or SNS destination receives a
# JSON document of metadata about the failed batch (topic, partition, offsets, timestamps) and
# not the payload. Only S3 receives the entire invocation record. So "route failures to SQS and
# redrive them" does not work: the records are not there. SQS gives you a pointer to replay from
# Kafka; S3 gives you the record itself, which is what you need to read the malformed field that
# caused the failure.
#
# There is a cost consequence too. An on-failure destination requires a VPC endpoint for the
# destination service inside the Kafka cluster's VPC. S3 has a free gateway endpoint. SQS and SNS
# have none, so either would require a billed interface endpoint.
#
# One bucket serves every topic, partitioned by prefix. The failure volume of a sample does not
# justify a bucket each.

resource "aws_s3_bucket" "failures" {
  bucket_prefix = "${var.name_prefix}-failures-"

  # Objects here are failed records, not durable state: a reader must be able to destroy the
  # stack without emptying the bucket by hand first.
  force_destroy = true

  tags = { Name = "${var.name_prefix}-failures" }
}

# This bucket holds FULL MESSAGE PAYLOADS, so it inherits the sensitivity of the events flowing
# through the topic. That makes the three controls below non-optional rather than boilerplate.
resource "aws_s3_bucket_public_access_block" "failures" {
  bucket = aws_s3_bucket.failures.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "failures" {
  bucket = aws_s3_bucket.failures.id

  rule {
    apply_server_side_encryption_by_default {
      # SSE-S3. A customer-managed key would be the production choice; for a teaching sample the
      # AWS-managed key keeps the IAM story readable, and the README says so rather than leaving
      # it to be discovered.
      sse_algorithm = "AES256"
    }
  }
}

# Bounds how long payloads persist. Without this the sensitivity noted above has no time limit.
resource "aws_s3_bucket_lifecycle_configuration" "failures" {
  bucket = aws_s3_bucket.failures.id

  rule {
    id     = "expire-failed-records"
    status = "Enabled"

    filter {}

    expiration {
      days = var.dlq_expiration_days
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# Deny any request that does not arrive over TLS. The bucket holds full message payloads.
data "aws_iam_policy_document" "failures_bucket" {
  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]
    resources = [
      aws_s3_bucket.failures.arn,
      "${aws_s3_bucket.failures.arn}/*",
    ]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "failures" {
  bucket = aws_s3_bucket.failures.id
  policy = data.aws_iam_policy_document.failures_bucket.json

  # Public access block first, so S3 never evaluates this policy without it in place.
  depends_on = [aws_s3_bucket_public_access_block.failures]
}

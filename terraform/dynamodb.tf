# Two tables per topic, and the separation is the point.
#
# read model   the projection consumers query
# ledger       dedup marker and attempt counter, one item per topic-partition-offset, TTL'd
#
# The ledger is the part of the inbox pattern that remains application code. Kafka's log replaces
# the durable landing, consumer-group offsets replace progress tracking, and the on-failure
# destination replaces failure quarantine. None of them records whether this handler already
# applied a given record, and delivery is at-least-once.
#
# Per topic rather than shared. One table serving every topic couples them: housekeeping cost
# scales with the total, and no stream can be operated on without touching the others.

resource "aws_dynamodb_table" "read_model" {
  for_each = var.topics

  name         = "${var.name_prefix}-${each.key}-read-model"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"

  attribute {
    name = "pk"
    type = "S"
  }

  # Encrypted with a customer-managed key rather than the AWS-owned default. The owned key cannot
  # be audited, granted or revoked; a CMK appears in CloudTrail and its access is revocable by
  # editing the key policy. See kms.tf.
  server_side_encryption {
    enabled     = true
    kms_key_arn = aws_kms_key.dynamodb.arn
  }

  # Continuous backups. These tables are rebuildable from the topic, but only while the records are
  # still within the topic's retention window, and that window is often set by whoever owns the
  # topic rather than by the consumer. Recovery to a point in time does not depend on retention.
  point_in_time_recovery {
    enabled = true
  }

  # DESTROY rather than the RETAIN default: a sample a reader cannot cleanly tear down is a
  # sample that leaves a bill behind.
  lifecycle {
    prevent_destroy = false
  }

  tags = { Name = "${var.name_prefix}-${each.key}-read-model" }
}

resource "aws_dynamodb_table" "ledger" {
  for_each = var.topics

  name         = "${var.name_prefix}-${each.key}-ledger"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"

  attribute {
    name = "pk"
    type = "S"
  }

  # Encrypted with a customer-managed key rather than the AWS-owned default. The owned key cannot
  # be audited, granted or revoked; a CMK appears in CloudTrail and its access is revocable by
  # editing the key policy. See kms.tf.
  server_side_encryption {
    enabled     = true
    kms_key_arn = aws_kms_key.dynamodb.arn
  }

  # Continuous backups. These tables are rebuildable from the topic, but only while the records are
  # still within the topic's retention window, and that window is often set by whoever owns the
  # topic rather than by the consumer. Recovery to a point in time does not depend on retention.
  point_in_time_recovery {
    enabled = true
  }

  # TTL replaces a scheduled cleanup job. Two caveats: deletion is best-effort within a couple of
  # days rather than prompt, and expired items remain readable until physically removed, so reads
  # must not assume an expired item is gone.
  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }

  tags = { Name = "${var.name_prefix}-${each.key}-ledger" }
}

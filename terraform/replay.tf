# Blue-green replay: rebuild a read model into a new table, verify it, then switch readers.
#
# A replay is always a new mapping rather than a reconfigured one, for four reasons:
#
#   1. starting_position, consumer_group_id and topics are create-only on an event source mapping.
#   2. Lambda ignores starting_position when the consumer group already has committed offsets, and
#      resumes from those offsets instead. A replay needs a group that has never consumed.
#   3. A mapping cannot pass configuration to its function, so writing to a different table
#      requires a second function running the same code with different environment variables.
#   4. AWS allows one mapping per (cluster, function, topic) triple. A second returns
#      ResourceConflictException naming the existing UUID.
#
# Retention bounds how far back a replay can reach. Beyond that window an archive is required.
#
# Setting enable_replay = false destroys the replay mapping and stops its Event Poller Unit charge.

resource "aws_dynamodb_table" "replay_read_model" {
  for_each = var.enable_replay ? var.topics : {}

  name         = "${var.name_prefix}-${each.key}-read-model-replay"
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

  tags = { Name = "${var.name_prefix}-${each.key}-read-model-replay" }
}

# The replay path's own ledger. See the LEDGER_TABLE comment below.
resource "aws_dynamodb_table" "replay_ledger" {
  for_each = var.enable_replay ? var.topics : {}

  name         = "${var.name_prefix}-${each.key}-ledger-replay"
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

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }

  tags = { Name = "${var.name_prefix}-${each.key}-ledger-replay" }
}

resource "aws_cloudwatch_log_group" "replay" {
  for_each = var.enable_replay ? var.topics : {}

  name              = "/aws/lambda/${var.name_prefix}-${each.key}-replay"
  retention_in_days = var.log_retention_days

  tags = { Name = "${var.name_prefix}-${each.key}-replay" }
}

# Same package and handler as the live projection; only the target table differs.
resource "aws_lambda_function" "replay" {
  for_each = var.enable_replay ? var.topics : {}

  function_name = "${var.name_prefix}-${each.key}-replay"
  role          = aws_iam_role.projection[each.key].arn
  handler       = "projection.handler.lambda_handler"
  runtime       = "python3.12"
  architectures = ["arm64"]

  filename         = data.archive_file.projection.output_path
  source_code_hash = data.archive_file.projection.output_base64sha256

  timeout     = var.function_timeout_seconds
  memory_size = 256

  # An explicit ceiling on top of the one the mapping already imposes (pollers are capped at the
  # partition count). Bounds the blast radius of a backlog drain on everything else in the account.
  reserved_concurrent_executions = var.reserved_concurrency

  environment {
    variables = {
      # A separate ledger, not the live one. Sharing it would mark every record the live path has
      # already processed as a duplicate, so the replay would skip almost everything and the new
      # table would stay empty. A rebuild writes to a table with no history, so there are no
      # duplicates to suppress.
      #
      # The replay still needs its own ledger: at-least-once delivery applies to the replay
      # mapping too, and a poison record causes redelivery of its batch tail.
      LEDGER_TABLE     = aws_dynamodb_table.replay_ledger[each.key].name
      READ_MODEL_TABLE = aws_dynamodb_table.replay_read_model[each.key].name
      LEDGER_TTL_DAYS  = tostring(var.ledger_ttl_days)
      MAX_ATTEMPTS     = tostring(var.max_attempts)
      LOG_LEVEL        = "INFO"
    }
  }

  depends_on = [aws_cloudwatch_log_group.replay]

  tags = { Name = "${var.name_prefix}-${each.key}-replay" }
}

resource "aws_lambda_event_source_mapping" "replay" {
  for_each = var.enable_replay ? var.topics : {}

  event_source_arn = var.msk_cluster_arn
  function_name    = aws_lambda_function.replay[each.key].arn
  topics           = [each.value.topic_name]

  # TRIM_HORIZON: a full rebuild from the start of the topic, bounded by retention.
  #
  # AT_TIMESTAMP (with starting_position_timestamp) also works on an MSK source and reads only the
  # records at or after that time, so a windowed replay is possible. It applies only to a consumer
  # group that has never consumed; see point 2 at the top of this file.
  starting_position = "TRIM_HORIZON"

  # Smaller than the live mapping, so a backfill does not compete with live traffic for throughput.
  batch_size = 10

  provisioned_poller_config {
    minimum_pollers = 1
    maximum_pollers = 1
  }

  filter_criteria {
    filter {
      pattern = jsonencode({
        value = {
          eventType = [each.value.event_type]
        }
      })
    }
  }

  function_response_types = ["ReportBatchItemFailures"]
  maximum_retry_attempts  = var.maximum_retry_attempts

  destination_config {
    on_failure {
      destination_arn = aws_s3_bucket.failures.arn
    }
  }

  # A consumer group that has never consumed. Reusing the live group id would make Lambda resume
  # from its committed offsets instead of the configured starting position.
  amazon_managed_kafka_event_source_config {
    consumer_group_id = "${var.name_prefix}-${each.key}-replay"
  }

  # enabled is the kill switch: set it false to stop a replay immediately, with no deploy.
  enabled = true

  depends_on = [
    aws_iam_role_policy.projection,
    aws_iam_role_policy.failure_destination,
  ]
}

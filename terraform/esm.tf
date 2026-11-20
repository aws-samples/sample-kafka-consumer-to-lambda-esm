# The event source mapping. Polling, partition assignment, work claiming, retry bookkeeping and
# dead-lettering are all expressed here as configuration rather than written as code.
#
# Several of these settings are commonly misconfigured. Each is explained at the point it appears.

resource "aws_lambda_event_source_mapping" "projection" {
  for_each = var.topics

  event_source_arn  = var.msk_cluster_arn
  function_name     = aws_lambda_function.projection[each.key].arn
  topics            = [each.value.topic_name]
  starting_position = "TRIM_HORIZON"
  batch_size        = var.batch_size
  enabled           = true

  # Provisioned mode is chosen for networking, not throughput.
  #
  # In on-demand mode the mapping invokes the function over a path through the cluster's VPC, so a
  # private-subnet deployment requires a NAT gateway or PrivateLink endpoints for Lambda and STS.
  # In provisioned mode Lambda handles that connection and needs neither.
  #
  # Two workarounds that look correct do not help. The function always runs in a Lambda-managed
  # VPC, so moving it out of the customer VPC changes nothing, and S3 or DynamoDB gateway
  # endpoints serve the handler's own egress rather than the invoke path. On-demand mode without
  # NAT or PrivateLink polls successfully and never invokes, with nothing in the function's logs.
  #
  # Cost: one Event Poller Unit billed continuously, about USD 0.185/hour.
  provisioned_poller_config {
    minimum_pollers = var.minimum_pollers
    maximum_pollers = var.maximum_pollers
  }

  # Filter criteria match the message value only, never Kafka headers.
  #
  # The pattern is {"value": {...}}; there is no {"headers": {...}} equivalent. A producer that
  # sets its event type in a Kafka header alone cannot be filtered declaratively. The alternatives
  # are to write the type into the value as well, or to use one topic per type.
  #
  # Filtering applies only to UTF-8 JSON or plain-string values. Avro and Protobuf bodies need the
  # schema-registry path.
  #
  # Unmatched records are dropped rather than retried, and offsets are committed for matched and
  # unmatched records alike, so no backlog accumulates behind a filter.
  filter_criteria {
    filter {
      pattern = jsonencode({
        value = {
          eventType = [each.value.event_type]
        }
      })
    }
  }

  # Per-record isolation is off by default. Without this, one failing record causes the whole batch
  # to be retried; with it, the handler reports only the failures and only those are redelivered.
  #
  # The handler must return the right identifier shape in exchange. For Kafka sources it is an
  # object, {"partition": "<topic>-<partition>", "offset": <int>}, not the plain string used by
  # Kinesis, DynamoDB Streams and SQS. An identifier Lambda cannot match against the invoked event
  # is treated as a failure and the whole batch is retried, which silently undoes this setting.
  # See src/projection/records.py::failure_identifier.
  function_response_types = ["ReportBatchItemFailures"]

  # Split a failing batch and retry each half separately, narrowing which records are redelivered.
  # Off by default; exposed as a variable so the difference can be demonstrated.
  bisect_batch_on_function_error = var.bisect_batch_on_function_error

  # Retry budget before a record goes to the on-failure destination. This is the intended exit for
  # a poison record, rather than the handler giving up. Infinite retries combined with a
  # destination makes Lambda apply a cap of 10 regardless.
  maximum_retry_attempts = var.maximum_retry_attempts

  # Only S3 receives the full record.
  #
  # SQS and SNS destinations receive metadata about the failed batch: topic, partition, offsets and
  # timestamps, but not the payload. A runbook that redrives records out of an SQS DLQ has nothing
  # to redrive; it has offsets to replay from Kafka. S3 carries the record itself.
  #
  # A Kafka topic (kafka://topic-name) is also a valid destination.
  destination_config {
    on_failure {
      destination_arn = aws_s3_bucket.failures.arn
    }
  }

  # A stable consumer group id per topic. Left unset, Lambda generates one, and the generated value
  # changes if the mapping is replaced, which resets the consumption position.
  #
  # This, topics and starting_position are create-only and cannot be changed in place. That is why
  # a replay is a new mapping rather than a reconfigured one. See replay.tf.
  amazon_managed_kafka_event_source_config {
    consumer_group_id = "${var.name_prefix}-${each.key}"
  }

  # OffsetLag is emitted automatically. These groups are opt-in and add per-mapping event and error
  # counts plus MaxOffsetLag and SumOffsetLag across partitions, which is enough to alarm on lag
  # without custom instrumentation.
  metrics_config {
    metrics = ["EventCount", "ErrorCount", "KafkaMetrics"]
  }

  depends_on = [
    aws_iam_role_policy.projection,
    aws_iam_role_policy.failure_destination,
  ]
}

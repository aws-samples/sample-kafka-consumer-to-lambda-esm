# Consumer lag, alarmed, replacing a homegrown lag monitor.
#
# OffsetLag is emitted automatically in the AWS/Lambda namespace. MaxOffsetLag and SumOffsetLag
# arrive through the opt-in KafkaMetrics group enabled on the mapping. Neither needs handler
# instrumentation or a metric-publishing IAM grant.
#
# Lag is a recoverability signal as much as a performance one: when topic retention is set
# elsewhere, the retention window is the budget for detecting and fixing broken processing.

resource "aws_sns_topic" "alerts" {
  name_prefix = "${var.name_prefix}-alerts-"

  # Encrypted with the AWS-managed SNS key, which needs no key policy of its own.
  #
  # A customer-managed CMK would be the production choice and does require a key policy permitting
  # CloudWatch to publish. That is out of scope here.
  kms_master_key_id = "alias/aws/sns"

  tags = { Name = "${var.name_prefix}-alerts" }
}

resource "aws_sns_topic_subscription" "alerts_email" {
  count = var.alarm_email == "" ? 0 : 1

  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alarm_email
}

resource "aws_cloudwatch_metric_alarm" "offset_lag" {
  for_each = var.topics

  alarm_name        = "${var.name_prefix}-${each.key}-offset-lag"
  alarm_description = "Consumer lag on ${each.value.topic_name}. Sustained lag against a fixed retention window means data becomes unrecoverable rather than merely late."

  namespace   = "AWS/Lambda"
  metric_name = "OffsetLag"
  statistic   = "Maximum"

  # OffsetLag is published with a FunctionName dimension, and it is the exception. Every opt-in
  # event source mapping metric (FilteredOutEventCount, OnFailureDestinationDeliveredEventCount,
  # MaxOffsetLag, SumOffsetLag, PolledEventCount) is dimensioned by EventSourceMappingUUID
  # instead. See the alarm below.
  dimensions = {
    FunctionName = aws_lambda_function.projection[each.key].function_name
  }

  comparison_operator = "GreaterThanThreshold"
  threshold           = var.alarm_offset_lag_threshold
  period              = 60
  evaluation_periods  = 5
  treat_missing_data  = "notBreaching"

  alarm_actions = [aws_sns_topic.alerts.arn]
  ok_actions    = [aws_sns_topic.alerts.arn]

  tags = { Name = "${var.name_prefix}-${each.key}-offset-lag" }
}

# A separate signal from lag. Lag reports falling behind; this reports records being abandoned.
resource "aws_cloudwatch_metric_alarm" "on_failure_deliveries" {
  for_each = var.topics

  alarm_name        = "${var.name_prefix}-${each.key}-on-failure-deliveries"
  alarm_description = "Records delivered to the S3 on-failure destination for ${each.value.topic_name}. The mapping's retry budget was exhausted."

  namespace   = "AWS/Lambda"
  metric_name = "OnFailureDestinationDeliveredEventCount"
  statistic   = "Sum"

  # EventSourceMappingUUID, not FunctionName.
  #
  # The two metric families are dimensioned differently: OffsetLag by FunctionName, and every
  # opt-in ESM metric by EventSourceMappingUUID. An alarm written against the wrong dimension
  # matches nothing and stays in INSUFFICIENT_DATA, which is indistinguishable from a system that
  # never fails.
  dimensions = {
    EventSourceMappingUUID = aws_lambda_event_source_mapping.projection[each.key].uuid
  }

  comparison_operator = "GreaterThanThreshold"
  threshold           = 0
  period              = 300
  evaluation_periods  = 1
  treat_missing_data  = "notBreaching"

  alarm_actions = [aws_sns_topic.alerts.arn]

  tags = { Name = "${var.name_prefix}-${each.key}-on-failure" }
}

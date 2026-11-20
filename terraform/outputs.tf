output "read_model_tables" {
  description = "Logical topic name -> read-model table name."
  value       = { for k, t in aws_dynamodb_table.read_model : k => t.name }
}

output "ledger_tables" {
  description = "Logical topic name -> dedup/attempt-ledger table name."
  value       = { for k, t in aws_dynamodb_table.ledger : k => t.name }
}

output "functions" {
  description = "Logical topic name -> projection function name."
  value       = { for k, f in aws_lambda_function.projection : k => f.function_name }
}

output "event_source_mapping_uuids" {
  description = <<-EOT
    Logical topic name -> mapping UUID. Pass to `aws lambda get-event-source-mapping` to confirm a
    mapping is consuming rather than merely existing; LastProcessingResult is the field to read.
  EOT
  value       = { for k, m in aws_lambda_event_source_mapping.projection : k => m.uuid }
}

output "failure_bucket" {
  description = <<-EOT
    Bucket receiving failed records. Contains full payloads, which only an S3 destination provides,
    so it inherits the sensitivity of the events.
  EOT
  value       = aws_s3_bucket.failures.bucket
}

output "alerts_topic_arn" {
  description = "SNS topic carrying the lag and on-failure alarms."
  value       = aws_sns_topic.alerts.arn
}

output "replay" {
  description = "Replay path state. Empty unless enable_replay is true."
  value = var.enable_replay ? {
    rebuild_from = "TRIM_HORIZON"
    tables       = { for k, t in aws_dynamodb_table.replay_read_model : k => t.name }
    functions    = { for k, f in aws_lambda_function.replay : k => f.function_name }
    mappings     = { for k, m in aws_lambda_event_source_mapping.replay : k => m.uuid }
  } : null
}

output "teardown_order" {
  description = "Surfaced on every apply because getting it wrong wedges the prerequisite stack."
  value       = "Destroy THIS stack before the MSK cluster. The mapping's network interfaces live in the cluster's subnets and block subnet and VPC deletion while the mapping exists."
}

output "producer_function" {
  description = <<-EOT
    The test producer. VPC-attached, so it can reach brokers that nothing outside the VPC can:

      aws lambda invoke --function-name <this> --cli-binary-format raw-in-base64-out \
        --payload '{"scenario":"header_only"}' /dev/stdout

    Scenarios: matching, non_matching, header_only, poison, isolation, stale, ordering.
    `scripts/run_demo.sh <scenario>` wraps this and prints where to look for the evidence.
  EOT
  value       = aws_lambda_function.producer.function_name
}

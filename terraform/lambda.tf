# One function per topic, packaged from src/ so no build tooling is needed to try this.
#
# Not VPC-attached. The function reaches DynamoDB over the AWS network, and the mapping's poller,
# which does sit in the cluster's subnets, is managed by Lambda rather than by the function.
# Attaching the function would add ENI lifecycle to every deploy and every destroy.

# Unlike producer.tf, this package has no dependencies and is zipped straight from source with no
# build step. The projection needs only the standard library and boto3, which the runtime provides.

data "archive_file" "projection" {
  type = "zip"

  # src/ rather than src/projection/, so the zip contains the package directory and the handler
  # path projection.handler.lambda_handler resolves.
  source_dir  = "${path.module}/../src"
  output_path = "${path.module}/.terraform/projection.zip"
  excludes    = ["**/__pycache__/**", "**/*.pyc"]
}

# Created here, not by Lambda on first invocation. A log group Lambda creates implicitly is not
# in Terraform state, so it survives `destroy` and quietly accrues retention forever.
resource "aws_cloudwatch_log_group" "projection" {
  for_each = var.topics

  name              = "/aws/lambda/${var.name_prefix}-${each.key}-projection"
  retention_in_days = var.log_retention_days

  tags = { Name = "${var.name_prefix}-${each.key}-projection" }
}

resource "aws_lambda_function" "projection" {
  for_each = var.topics

  function_name = "${var.name_prefix}-${each.key}-projection"
  role          = aws_iam_role.projection[each.key].arn
  handler       = "projection.handler.lambda_handler"
  runtime       = "python3.12"
  architectures = ["arm64"]

  filename         = data.archive_file.projection.output_path
  source_code_hash = data.archive_file.projection.output_base64sha256

  # Kafka event source mappings cap the function timeout at 14 minutes rather than the usual 15.
  # The variable validation enforces that ceiling.
  timeout     = var.function_timeout_seconds
  memory_size = 256

  # An explicit ceiling on top of the one the mapping already imposes (pollers are capped at the
  # partition count). Bounds the blast radius of a backlog drain on everything else in the account.
  reserved_concurrent_executions = var.reserved_concurrency

  environment {
    variables = {
      LEDGER_TABLE     = aws_dynamodb_table.ledger[each.key].name
      READ_MODEL_TABLE = aws_dynamodb_table.read_model[each.key].name
      LEDGER_TTL_DAYS  = tostring(var.ledger_ttl_days)
      MAX_ATTEMPTS     = tostring(var.max_attempts)
      LOG_LEVEL        = "INFO"
    }
  }

  depends_on = [aws_cloudwatch_log_group.projection]

  tags = { Name = "${var.name_prefix}-${each.key}-projection" }
}

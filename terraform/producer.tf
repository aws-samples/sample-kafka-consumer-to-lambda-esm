# The test producer.
#
# This is the one function in the sample that is VPC-ATTACHED, and the contrast with the
# projection function is worth noticing rather than glossing over:
#
#   projection   not in the VPC. It talks to DynamoDB over the AWS network and never touches a
#                broker, because the event source mapping's poller does that on its behalf.
#
#   producer     in the VPC, in the CLUSTER's security group. It is an ordinary Kafka client, so
#                it has to be where the brokers are: private subnets, no NAT, no public access.
#
# That asymmetry is part of what the managed pipeline actually buys: the consumer side stops
# needing VPC placement, ENI lifecycle and broker reachability, and only the producer still does.
#
# Invoked from a laptop with nothing but AWS credentials:
#
#   aws lambda invoke --function-name <prefix>-producer \
#       --payload '{"scenario":"header_only"}' --cli-binary-format raw-in-base64-out /dev/stdout

# Dependencies are installed into a build directory at plan time. `kafka-python` and the MSK IAM
# signer are not in the Lambda runtime, so unlike the projection package this one cannot simply
# be zipped from source.
#
# `terraform_data` rather than `null_resource`: it is the supported replacement and needs no
# extra provider.
resource "terraform_data" "producer_build" {
  # Rebuild when the source or the pinned dependencies change. Without the requirements hash a
  # dependency bump would leave a stale zip in place with no indication why behaviour changed.
  triggers_replace = [
    filesha256("${path.module}/../src/producer/handler.py"),
    filesha256("${path.module}/../src/producer/scenarios.py"),
    filesha256("${path.module}/../src/producer/requirements.txt"),
  ]

  provisioner "local-exec" {
    command = <<-EOT
      set -e
      BUILD="${path.module}/.terraform/producer-build"
      rm -rf "$BUILD"
      mkdir -p "$BUILD"
      cp "${path.module}"/../src/producer/*.py "$BUILD/"

      # Cross-compile for the function's architecture rather than the build machine's. Both
      # dependencies are pure Python, so --only-binary succeeds with py3-none-any wheels; the
      # flags matter if a future dependency ships native code.
      python3 -m pip install \
        --quiet --no-compile --upgrade \
        --platform manylinux2014_aarch64 \
        --implementation cp \
        --only-binary=:all: \
        --target "$BUILD" \
        -r "${path.module}/../src/producer/requirements.txt"

      # Drop the AWS SDK. The MSK IAM signer depends on boto3, which pulls in botocore: 28 MB of a
      # 29 MB package, all already present in the Lambda runtime. Pruning takes the upload under
      # 1 MB.
      #
      # Only the SDK itself is removed. botocore's small pure-Python dependencies (urllib3,
      # dateutil, jmespath, six) stay at the versions this botocore release requires, and the
      # runtime's botocore accepts them. If the signer fails to import after a runtime change,
      # remove this block first.
      rm -rf "$BUILD"/boto3 "$BUILD"/boto3-*.dist-info \
             "$BUILD"/botocore "$BUILD"/botocore-*.dist-info \
             "$BUILD"/s3transfer "$BUILD"/s3transfer-*.dist-info \
             "$BUILD"/bin
    EOT
  }
}

data "archive_file" "producer" {
  type        = "zip"
  source_dir  = "${path.module}/.terraform/producer-build"
  output_path = "${path.module}/.terraform/producer.zip"
  # dist-info directories are NOT excluded. kafka-python reads its own version through package
  # metadata, so stripping them to save a few hundred KB breaks the import with
  # "No package metadata was found for kafka-python-ng": again only at invocation time.
  excludes = ["**/__pycache__/**", "**/*.pyc"]

  depends_on = [terraform_data.producer_build]
}

resource "aws_cloudwatch_log_group" "producer" {
  name              = "/aws/lambda/${var.name_prefix}-producer"
  retention_in_days = var.log_retention_days

  tags = { Name = "${var.name_prefix}-producer" }
}

resource "aws_iam_role" "producer" {
  name_prefix        = "${var.name_prefix}-producer-"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json

  tags = { Name = "${var.name_prefix}-producer" }
}

data "aws_iam_policy_document" "producer" {
  # ec2:DeleteNetworkInterface is granted on "*" because Lambda validates it against an unscoped
  # resource when it creates the function and the event source mapping; a scoped grant is rejected.
  # CreateNetworkInterface stays scoped to the cluster's subnets and security group.
  # checkov:skip=CKV_AWS_111:Lambda rejects a resource-scoped ec2:DeleteNetworkInterface; see comment above.
  # checkov:skip=CKV_AWS_356:Lambda rejects a resource-scoped ec2:DeleteNetworkInterface; see comment above.
  # Write, not read. The producer has no business consuming, and scoping it this way makes the
  # demo scenarios incapable of quietly doing something the projection was supposed to do.
  statement {
    sid       = "KafkaCluster"
    effect    = "Allow"
    actions   = ["kafka-cluster:Connect", "kafka-cluster:DescribeCluster"]
    resources = [var.msk_cluster_arn]
  }

  statement {
    sid    = "KafkaTopicWrite"
    effect = "Allow"
    actions = [
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:WriteData",
    ]
    resources = [for t in var.topics : "${local.topic_arn_prefix}/${t.topic_name}"]
  }

  # Required because this function is VPC-attached, and split by whether the action can be scoped.
  # The Describe actions do not support resource-level permissions and are read-only; the two
  # mutating actions are scoped to the subnets and security group this function is placed in.
  statement {
    sid    = "DescribeNetworkingReadOnly"
    effect = "Allow"
    actions = [
      "ec2:DescribeNetworkInterfaces",
      "ec2:DescribeSecurityGroups",
      "ec2:DescribeSubnets",
      "ec2:DescribeVpcs",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "ManageNetworkInterfaces"
    effect = "Allow"
    actions = [
      "ec2:CreateNetworkInterface",
    ]
    resources = concat(
      ["${local.ec2_arn_prefix}:network-interface/*"],
      [for id in var.private_subnet_ids : "${local.ec2_arn_prefix}:subnet/${id}"],
      ["${local.ec2_arn_prefix}:security-group/${var.cluster_security_group_id}"],
    )
  }

  # Lambda validates ec2:DeleteNetworkInterface at function and mapping creation against an
  # unscoped resource, so a resource-scoped grant fails with "Cannot delete ENIs ...".
  statement {
    sid       = "DeleteNetworkInterfaces"
    effect    = "Allow"
    actions   = ["ec2:DeleteNetworkInterface"]
    resources = ["*"]
  }

  statement {
    sid       = "Logs"
    effect    = "Allow"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.producer.arn}:*"]
  }
}

resource "aws_iam_role_policy" "producer" {
  name   = "producer"
  role   = aws_iam_role.producer.id
  policy = data.aws_iam_policy_document.producer.json
}

resource "aws_lambda_function" "producer" {
  function_name = "${var.name_prefix}-producer"
  role          = aws_iam_role.producer.arn
  handler       = "handler.lambda_handler"
  runtime       = "python3.12"
  architectures = ["arm64"]

  filename         = data.archive_file.producer.output_path
  source_code_hash = data.archive_file.producer.output_base64sha256

  # Generous: the first invocation pays a cold start plus broker discovery and the SASL/IAM
  # handshake, and the ordering scenario produces ten records synchronously.
  timeout     = 120
  memory_size = 512

  # The producer is invoked by hand to drive a demo, so a low ceiling is correct: it stops a
  # mistyped loop from producing at scale into a cluster a reader is paying for.
  reserved_concurrent_executions = 2

  # In the VPC, in the cluster's own security group, which already permits the broker ports
  # from itself, so no additional rule is needed. Reusing the cluster group rather than creating
  # a second one keeps the producer subject to exactly the reachability the cluster grants.
  vpc_config {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = [var.cluster_security_group_id]
  }

  environment {
    variables = {
      BOOTSTRAP_BROKERS  = var.msk_bootstrap_brokers_sasl_iam
      DEFAULT_TOPIC      = values(var.topics)[0].topic_name
      DEFAULT_EVENT_TYPE = values(var.topics)[0].event_type
      LOG_LEVEL          = "INFO"
    }
  }

  depends_on = [aws_cloudwatch_log_group.producer]

  tags = { Name = "${var.name_prefix}-producer" }
}

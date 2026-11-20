# One execution role per topic.
#
# A single shared role would be shorter and would also mean the orders function could write the
# shipments read model. Per-topic roles keep the isolation claim honest at the IAM layer, not
# just the resource layer.
#
# The permissions fall into four groups, and the third is the one that surprises people.

data "aws_iam_policy_document" "assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "projection" {
  for_each = var.topics

  name_prefix        = "${var.name_prefix}-${each.key}-"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json

  tags = { Name = "${var.name_prefix}-${each.key}" }
}

data "aws_iam_policy_document" "projection" {
  for_each = var.topics
  # ec2:DeleteNetworkInterface is granted on "*" because Lambda validates it against an unscoped
  # resource when it creates the function and the event source mapping; a scoped grant is rejected.
  # CreateNetworkInterface stays scoped to the cluster's subnets and security group.
  # checkov:skip=CKV_AWS_111:Lambda rejects a resource-scoped ec2:DeleteNetworkInterface; see comment above.
  # checkov:skip=CKV_AWS_356:Lambda rejects a resource-scoped ec2:DeleteNetworkInterface; see comment above.

  # 1. Kafka data plane. With IAM authentication the execution role IS the Kafka client
  #    identity, which is why there is no SASL password anywhere in this repository. Scoped to
  #    this cluster, this topic and this consumer group rather than the cluster wildcard.
  statement {
    sid    = "KafkaCluster"
    effect = "Allow"
    actions = [
      "kafka-cluster:Connect",
      "kafka-cluster:DescribeCluster",
      "kafka-cluster:DescribeClusterDynamicConfiguration",
    ]
    resources = [var.msk_cluster_arn]
  }

  statement {
    sid    = "KafkaTopic"
    effect = "Allow"
    actions = [
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:DescribeTopicDynamicConfiguration",
      "kafka-cluster:ReadData",
    ]
    resources = ["${local.topic_arn_prefix}/${each.value.topic_name}"]
  }

  # Scoped to this topic's live group and, when replay is enabled, its replay group. Both mappings
  # run under this role. If a mapping reports an authorisation error naming the group, Lambda is
  # suffixing the id: change each entry to end in "*", which still excludes other topics' groups.
  statement {
    sid    = "KafkaGroup"
    effect = "Allow"
    actions = [
      "kafka-cluster:AlterGroup",
      "kafka-cluster:DescribeGroup",
    ]
    resources = concat(
      ["${local.group_arn_prefix}/${var.name_prefix}-${each.key}"],
      var.enable_replay ? ["${local.group_arn_prefix}/${var.name_prefix}-${each.key}-replay"] : [],
    )
  }

  # 2. Control plane. The mapping needs to resolve brokers from the cluster ARN.
  statement {
    sid    = "KafkaControlPlane"
    effect = "Allow"
    actions = [
      "kafka:DescribeClusterV2",
      "kafka:GetBootstrapBrokers",
    ]
    resources = [var.msk_cluster_arn]
  }

  # 3. Network interfaces, split by whether the action can be scoped.
  #
  #    The function is not VPC-attached, so these look unnecessary. The event source mapping's
  #    poller runs inside the cluster's subnets and security group under this execution role, and
  #    that is what needs them.
  #
  #    The Describe actions do not support resource-level permissions, so their resource is "*".
  #    They are read-only. The two mutating actions do support resource-level permissions and are
  #    scoped to the network interfaces, subnets and security group this mapping may touch, so no
  #    write action here carries an unconstrained resource.
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

  # 4. This topic's two tables, and nothing else. No cross-topic access by construction.
  # Required because the tables are encrypted with a customer-managed key. Without these the
  # function fails on every read and write with an AccessDeniedException naming the key, not the
  # table.
  statement {
    sid    = "ReadModelAndLedgerKey"
    effect = "Allow"
    actions = [
      "kms:Decrypt",
      "kms:DescribeKey",
      "kms:Encrypt",
      "kms:GenerateDataKey",
      "kms:ReEncryptFrom",
      "kms:ReEncryptTo",
    ]
    resources = [aws_kms_key.dynamodb.arn]
  }

  statement {
    sid    = "ReadModelAndLedger"
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
    ]
    resources = concat(
      [aws_dynamodb_table.read_model[each.key].arn, aws_dynamodb_table.ledger[each.key].arn],
      var.enable_replay ? [
        aws_dynamodb_table.replay_read_model[each.key].arn,
        aws_dynamodb_table.replay_ledger[each.key].arn,
      ] : [],
    )
  }

  statement {
    sid       = "Logs"
    effect    = "Allow"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.projection[each.key].arn}:*"]
  }
}

resource "aws_iam_role_policy" "projection" {
  for_each = var.topics

  name   = "projection"
  role   = aws_iam_role.projection[each.key].id
  policy = data.aws_iam_policy_document.projection[each.key].json
}

# The on-failure destination is written by the MAPPING, not by the function, but the mapping acts
# under the function's execution role, so the grant belongs here.
#
# s3:PutObject alone is not sufficient. CreateEventSourceMapping validates the destination before
# the mapping becomes active and fails with
#
#   InvalidParameterValueException: The function execution role does not have permissions to
#   call ListBucket on S3
#
# which arrives several minutes into mapping creation, after the plan has looked clean. ListBucket
# is scoped to the bucket ARN; PutObject to the objects beneath it.
data "aws_iam_policy_document" "failure_destination" {
  statement {
    sid       = "InspectFailureBucket"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.failures.arn]
  }

  statement {
    sid       = "WriteFailedRecords"
    effect    = "Allow"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.failures.arn}/*"]
  }
}

resource "aws_iam_role_policy" "failure_destination" {
  for_each = var.topics

  name   = "failure-destination"
  role   = aws_iam_role.projection[each.key].id
  policy = data.aws_iam_policy_document.failure_destination.json
}

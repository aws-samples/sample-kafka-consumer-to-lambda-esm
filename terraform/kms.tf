# Customer-managed key for the DynamoDB tables.
#
# Both the read model and the ledger hold data derived from the message payload, so they inherit
# the sensitivity of the events flowing through the topic. The AWS-owned key DynamoDB uses by
# default cannot be audited, granted or revoked; a customer-managed key (CMK) can. CloudTrail
# records every Decrypt against it, access is revocable by editing the key policy, and the key
# can be scheduled for deletion independently of the tables.
#
# One key serves every topic's tables. A key per topic would strengthen the cross-topic isolation
# story the per-topic roles already make, but key-per-table carries a standing monthly charge each
# and rotation surface that the failure volume of a sample does not justify. The README notes this
# as the production upgrade point.
#
# key_rotation is enabled: AWS rotates the backing key material yearly with no change to the key id
# or the ciphertext references, so nothing downstream has to be re-encrypted.
resource "aws_kms_key" "dynamodb" {
  description             = "${var.name_prefix} DynamoDB read-model and ledger encryption"
  deletion_window_in_days = var.kms_key_deletion_window_days
  enable_key_rotation     = true
  policy                  = data.aws_iam_policy_document.dynamodb_key.json

  tags = { Name = "${var.name_prefix}-dynamodb" }
}

# An explicit key policy rather than the default.
#
# Omitting `policy` gives the key a default that grants the account root full control, which works
# but says nothing about intended use. Declaring it buys one real constraint: the second statement
# permits cryptographic operations only when the call arrives through DynamoDB. A principal granted
# kms:Decrypt by an IAM policy therefore cannot read ciphertext out of this key directly, only by
# querying the table, which is already governed by its own table permissions.
#
# The first statement grants the account root the management actions but no cryptographic ones. A
# key whose policy does not grant the account administrative access cannot be modified by anyone,
# including the account owner, and AWS cannot restore it.
data "aws_iam_policy_document" "dynamodb_key" {
  # A key policy's Resource is always "*". It refers to the key the policy is attached to, and KMS
  # accepts no other value, so the two unconstrained-resource checks cannot be satisfied here. The
  # skips are inline rather than in .ash.yaml so they apply to any scanner invocation, including
  # ones that do not read this repository's scanner config.
  #
  # checkov:skip=CKV_AWS_111:A KMS key policy's resource is always "*"; it denotes the key itself.
  # checkov:skip=CKV_AWS_356:A KMS key policy's resource is always "*"; it denotes the key itself.
  # checkov:skip=CKV_AWS_109:Console-default key administration (includes kms:PutKeyPolicy) for the account root, with no cryptographic actions. Narrowing the principal to a deployment role is production hardening, recorded in SECURITY.md.

  # Key administration without cryptographic operations: the console-default Key Administrators
  # statement. Create, rotate, alias, tag and delete are covered. Encrypt, Decrypt and
  # GenerateDataKey are deliberately absent, so the only route to them is the ViaService statement
  # below. That is what makes the claim above true.
  statement {
    sid    = "KeyAdministration"
    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = ["arn:${local.arn_parts[1]}:iam::${local.arn_parts[4]}:root"]
    }

    actions = [
      "kms:Create*", "kms:Describe*", "kms:Enable*", "kms:List*", "kms:Put*",
      "kms:Update*", "kms:Revoke*", "kms:Disable*", "kms:Get*", "kms:Delete*",
      "kms:TagResource", "kms:UntagResource",
      "kms:ScheduleKeyDeletion", "kms:CancelKeyDeletion",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "UseOnlyThroughDynamoDB"
    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = ["arn:${local.arn_parts[1]}:iam::${local.arn_parts[4]}:root"]
    }

    actions = [
      "kms:CreateGrant", # DynamoDB creates a grant when a table is associated with the key
      "kms:Decrypt",
      "kms:DescribeKey",
      "kms:Encrypt",
      "kms:GenerateDataKey*",
      "kms:ReEncrypt*",
    ]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["dynamodb.${local.arn_parts[3]}.amazonaws.com"]
    }
  }
}

# A readable alias so the key is identifiable in the console and in CloudTrail without pasting a
# UUID. Aliases are free and have no effect on access.
resource "aws_kms_alias" "dynamodb" {
  name          = "alias/${var.name_prefix}-dynamodb"
  target_key_id = aws_kms_key.dynamodb.key_id
}

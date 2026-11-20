terraform {
  required_version = ">= 1.9.0"

  required_providers {
    aws = {
      source = "hashicorp/aws"

      # 6.44.0 is the floor for `provisioned_poller_config` on
      # aws_lambda_event_source_mapping, which this root cannot do without: provisioned mode is
      # what removes the NAT gateway / PrivateLink requirement from the invoke path.
      #
      # The registry documentation describes function_response_types, maximum_retry_attempts and
      # bisect_batch_on_function_error as available only for stream sources, and scopes
      # filter_criteria to Kinesis, DynamoDB and SQS. The provider schema carries no source-type
      # gating on any of them and the API accepts all four on a Kafka source.
      version = ">= 6.44.0, < 7.0.0"
    }

    archive = {
      source  = "hashicorp/archive"
      version = ">= 2.6.0, < 3.0.0"
    }
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project = var.name_prefix
      Sample  = "msk-lambda-read-model"
      Root    = "pattern"
    }
  }
}

locals {
  # Everything the IAM policy needs is derived from the cluster ARN, so a reader supplies one
  # value rather than four, and this root needs no credentials to compute them, which keeps
  # `terraform plan` honest about what it is really waiting on.
  #
  # An MSK cluster ARN looks like:
  #
  #   arn:aws:kafka:us-east-1:111122223333:cluster/my-cluster/fc2f5bdf-...-2
  #
  # Topic and consumer-group ARNs share its account, region, cluster name and cluster UUID,
  # differing only in the resource type segment.
  arn_parts    = split(":", var.msk_cluster_arn)
  arn_prefix   = join(":", slice(local.arn_parts, 0, 5))
  cluster_path = join("/", slice(split("/", local.arn_parts[5]), 1, 3))

  topic_arn_prefix = "${local.arn_prefix}:topic/${local.cluster_path}"
  group_arn_prefix = "${local.arn_prefix}:group/${local.cluster_path}"

  # The cluster ARN also yields the partition, region and account needed to address EC2 resources,
  # which is how the network-interface permissions below are scoped rather than left as "*".
  ec2_arn_prefix = "arn:${local.arn_parts[1]}:ec2:${local.arn_parts[3]}:${local.arn_parts[4]}"
}

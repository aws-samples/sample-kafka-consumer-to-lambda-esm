variable "region" {
  description = "AWS region. Must match the region of the prerequisite cluster."
  type        = string
  default     = "us-east-1"
}

variable "name_prefix" {
  description = "Prefix for resource names."
  type        = string
  default     = "msk-readmodel"
}

# --- the seam -----------------------------------------------------------------
#
# These six values are the entire contract with the prerequisite environment. They are
# variables rather than a remote-state lookup on purpose: the primary case is a reader pointing
# this root at a cluster they already run. If this root ever reads sample-infra's state, or
# finds resources by naming convention, the split between the two has become decorative.
#
# Copy prereq.auto.tfvars.example to prereq.auto.tfvars and fill these in.

variable "msk_cluster_arn" {
  description = "ARN of the MSK cluster to consume from. Topic and consumer-group ARNs are derived from it."
  type        = string

  validation {
    condition     = can(regex("^arn:aws[a-z-]*:kafka:[a-z0-9-]+:[0-9]{12}:cluster/[^/]+/[^/]+$", var.msk_cluster_arn))
    error_message = "Expected an MSK cluster ARN of the form arn:aws:kafka:REGION:ACCOUNT:cluster/NAME/UUID."
  }
}

variable "msk_bootstrap_brokers_sasl_iam" {
  description = <<-EOT
    IAM (SASL) bootstrap brokers, port 9098. Not used by the event source mapping, which
    resolves brokers from the cluster ARN. This is here for the test producer and for anyone
    running an admin client against the same cluster.
  EOT
  type        = string
  default     = ""
}

variable "vpc_id" {
  description = "VPC hosting the cluster. Recorded for reference; this root creates nothing in it."
  type        = string
  default     = ""
}

variable "private_subnet_ids" {
  description = <<-EOT
    The cluster's client subnets. The event source mapping creates its network interfaces here,
    using the cluster's subnets and security group rather than the function's, which is why this
    root needs to know them even though the function is not VPC-attached.
  EOT
  type        = list(string)
  default     = []
}

variable "cluster_security_group_id" {
  description = <<-EOT
    The cluster security group. It must permit the broker port (9098 for IAM) from itself,
    because the mapping's network interfaces join this group. Owned by the prerequisite root;
    named here so the dependency is visible.
  EOT
  type        = string
  default     = ""
}

variable "topics" {
  description = <<-EOT
    One event source mapping, one function, one read model and one ledger per entry. The map
    key is the logical stream name and must match the prerequisite root's key.

    event_type is matched by the mapping's filter criteria against the message value. Filter
    criteria cannot read Kafka headers, so a producer that puts its discriminator in a header alone
    cannot be filtered declaratively.
  EOT
  type = map(object({
    topic_name = string
    event_type = string
  }))
}

# --- event source mapping -----------------------------------------------------

variable "batch_size" {
  description = "Records per invocation. MSK default is 100; maximum 10000."
  type        = number
  default     = 100
}

variable "minimum_pollers" {
  description = <<-EOT
    Provisioned-mode floor, 1-200.

    Provisioned mode is chosen for networking rather than throughput. In on-demand mode the mapping
    invokes the function over a path through the cluster's VPC, so a private-subnet deployment needs
    a NAT gateway or PrivateLink endpoints for Lambda and STS. In provisioned mode Lambda handles
    that connection and needs neither. The cost is one Event Poller Unit billed continuously, about
    USD 0.185/hour.

    Misconfigured on-demand mode fails silently: the mapping polls successfully and never invokes.
  EOT
  type        = number
  default     = 1
}

variable "maximum_pollers" {
  description = "Provisioned-mode ceiling, 1-2000."
  type        = number
  default     = 2
}

variable "maximum_retry_attempts" {
  description = <<-EOT
    Mapping-level retry budget before a record goes to the on-failure destination. -1 is infinite;
    infinite retries combined with a destination makes Lambda apply a cap of 10 regardless.

    This budget is the intended exit for a poison record, rather than the handler giving up.
  EOT
  type        = number
  default     = 3
}

variable "function_timeout_seconds" {
  description = <<-EOT
    Function timeout. Kafka event source mappings cap this at 14 minutes (840s) rather than the
    usual 15, and will refuse a function configured above the cap.
  EOT
  type        = number
  default     = 60

  validation {
    condition     = var.function_timeout_seconds <= 840
    error_message = "Kafka event source mappings support a maximum function timeout of 14 minutes (840s)."
  }
}

# --- handler behaviour --------------------------------------------------------

variable "max_attempts" {
  description = <<-EOT
    Handler-level attempt ceiling before a record is parked. 0 disables parking, which is the
    default: the mapping's retry budget and its S3 destination are then the only exit.

    Above zero, the handler takes ownership of the record away from the managed path. A parked
    record returns success, so the offset advances and the mapping does not deliver it to the
    on-failure destination.
  EOT
  type        = number
  default     = 0
}

variable "ledger_ttl_days" {
  description = <<-EOT
    How long dedup / attempt-ledger items live.

    TTL replaces a scheduled cleanup job, with two caveats: deletion is best-effort within a couple
    of days, and expired items remain readable until physically removed.

    It also bounds how far dedup protects you. A replay reaching further back than this is not
    suppressed, which is why precedence gating rather than dedup is what makes replay safe.
  EOT
  type        = number
  default     = 7
}

# --- failure destination and observability ------------------------------------

variable "kms_key_deletion_window_days" {
  description = <<-EOT
    Waiting period before a scheduled key deletion completes, 7 to 30 days.

    Set to the 7-day minimum here so a torn-down sample stops incurring the key charge quickly.
    Production should use a longer window, because a deleted key makes every item encrypted under
    it permanently unreadable.
  EOT
  type        = number
  default     = 7

  validation {
    condition     = var.kms_key_deletion_window_days >= 7 && var.kms_key_deletion_window_days <= 30
    error_message = "kms_key_deletion_window_days must be between 7 and 30."
  }
}

variable "dlq_expiration_days" {
  description = "Lifecycle expiry for failed records in S3. Bounds how long full payloads persist."
  type        = number
  default     = 30
}

variable "log_retention_days" {
  description = <<-EOT
    CloudWatch Logs retention. Set explicitly so destroy removes the groups instead of orphaning
    them.

    Defaults to 365 days: a year is the shortest retention that satisfies the common "retain logs
    for at least one year" control, and keeps an audit trail for the read models long enough to
    reconstruct what the projection did. Lower it for a throwaway demo if the log cost matters more
    than the trail.
  EOT
  type        = number
  default     = 365

  validation {
    condition = contains(
      [1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1096, 1827, 2192, 2557, 2922, 3288, 3653],
      var.log_retention_days
    )
    error_message = "retention_in_days must be one of the values CloudWatch Logs accepts (e.g. 1, 7, 30, 90, 365, 731, 1827, 3653)."
  }
}

variable "alarm_offset_lag_threshold" {
  description = <<-EOT
    OffsetLag value that raises the consumer-lag alarm.

    This is a recoverability control as much as a performance one. When topic retention is set by
    someone else, the retention window is the budget for detecting and fixing broken processing,
    and this alarm is what reports that the budget is being spent.
  EOT
  type        = number
  default     = 10000
}

variable "alarm_email" {
  description = "Optional email subscribed to the lag alarm topic. Empty disables the subscription."
  type        = string
  default     = ""
}

# --- replay -------------------------------------------------------------------

variable "enable_replay" {
  description = <<-EOT
    Stand up the blue-green replay path: a second read model, a separate ledger, a second function
    writing to them, and a replay mapping rebuilding from the start of the topic.

    Setting this back to false destroys the replay mapping and stops its poller charge.
  EOT
  type        = bool
  default     = false
}

# The replay rebuilds from the start of the topic (TRIM_HORIZON), bounded by retention. AT_TIMESTAMP
# also works on an MSK source for a windowed replay from a fresh consumer group; this sample does
# not expose it as a variable. See the starting_position comment in replay.tf.

variable "bisect_batch_on_function_error" {
  description = <<-EOT
    Split a failing batch in two and retry each half separately, narrowing which records are
    redelivered. Contrary to the registry documentation, which scopes this to "stream sources
    (DynamoDB and Kinesis)", it is a valid MSK event source mapping parameter.
  EOT
  type        = bool
  default     = false
}

variable "reserved_concurrency" {
  description = <<-EOT
    Reserved concurrent executions per projection function. -1 leaves it unreserved.

    Concurrency is already capped at the partition count by the event source mapping, so this is a
    second, explicit ceiling. It bounds what a replay or a backlog drain can take from the rest of
    the account.
  EOT
  type        = number
  default     = 10
}

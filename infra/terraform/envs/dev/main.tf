# Dev environment on real AWS.
#
# Split by cost, deliberately:
#
#   CHEAP TIER -- serverless, pay-per-request, ~$0/month when idle.
#     S3, SNS, SQS and ECR are always created. SES is optional.
#
#   FLAG-GATED -- billed per hour whether or not you use them. Default false.
#     enable_rds  ~$15/month   db.t4g.micro, single-AZ
#     enable_eks  ~$110/month  $73 control plane + 2x t3.small + NAT gateway
#
# `terraform destroy` when you finish a session. See docs/costs.md.

terraform {
  required_version = ">= 1.9"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # `make aws-init` supplies the account-scoped bucket and the remaining
  # settings created by infra/terraform/bootstrap. Keeping the backend active
  # prevents a real apply from quietly falling back to ignored local state.
  # Migrate older local state with `make aws-init AWS_INIT_ARGS=-migrate-state`.
  backend "s3" {}
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project     = "my-local-platform"
      Environment = "dev"
      ManagedBy   = "terraform"
      # Makes it trivial to spot anything this repo left running:
      #   aws resourcegroupstaggingapi get-resources --tag-filters Key=Project,Values=my-local-platform
      Ephemeral = "true"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  name   = "mlp-${var.environment}"
  suffix = data.aws_caller_identity.current.account_id
}

# =============================================================================
# Cheap tier -- always created
# =============================================================================

# Two trivy findings are accepted here rather than fixed.
#
# AWS-0132 (customer-managed KMS key): SSE-S3 (AES256) is on by default and is
# enough for throwaway build artifacts. A CMK costs $1/month per key plus
# request charges, to protect data that expires after 30 days.
#
# AWS-0090 (versioning): deliberately off. This bucket holds disposable build
# output with a 30-day expiry and force_destroy = true. The Terraform *state*
# bucket in bootstrap/ IS versioned, which is where it actually matters.
#
# The directives must be the LAST comment lines before the resource, with no
# prose between them and it -- trivy silently ignores a directive followed by
# another comment line.
#trivy:ignore:AWS-0132
#trivy:ignore:AWS-0090
resource "aws_s3_bucket" "artifacts" {
  bucket        = "${local.name}-artifacts-${local.suffix}"
  force_destroy = true # dev data; let destroy actually work
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket                  = aws_s3_bucket.artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id

  # Nothing in a dev bucket deserves to be paid for indefinitely.
  rule {
    id     = "expire-dev-objects"
    status = "Enabled"
    filter {}
    expiration {
      days = 30
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# The topic IS encrypted (see kms_master_key_id below), which cleared AWS-0095.
# AWS-0136 is the stricter follow-on rule asking for a *customer-managed* key.
# Accepted for the same reason as the buckets: a CMK is $1/month per key, and
# the AWS-managed key already encrypts at rest.
#trivy:ignore:AWS-0136
resource "aws_sns_topic" "events" {
  name = "${local.name}-events"

  # SNS has no free SSE equivalent to SQS's, so this uses the AWS-managed KMS
  # key. Storage is free; requests bill at ~$0.03 per 10,000. Negligible at
  # these volumes, but not literally zero.
  kms_master_key_id = "alias/aws/sns"
}

resource "aws_sqs_queue" "events_dlq" {
  name                      = "${local.name}-events-dlq"
  message_retention_seconds = 1209600 # 14 days, the maximum

  # SSE-SQS: encryption at rest with no KMS key and no request charges.
  # Genuinely free, so there is no reason to leave it off.
  sqs_managed_sse_enabled = true
}

resource "aws_sqs_queue" "events" {
  name = "${local.name}-events"

  sqs_managed_sse_enabled = true

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.events_dlq.arn
    maxReceiveCount     = 5
  })
}

resource "aws_sns_topic_subscription" "events_to_sqs" {
  topic_arn            = aws_sns_topic.events.arn
  protocol             = "sqs"
  endpoint             = aws_sqs_queue.events.arn
  raw_message_delivery = true
}

# SNS needs explicit permission to write to the queue; without this the
# subscription exists but every delivery silently fails.
resource "aws_sqs_queue_policy" "events" {
  queue_url = aws_sqs_queue.events.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "sns.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.events.arn
      Condition = {
        ArnEquals = { "aws:SourceArn" = aws_sns_topic.events.arn }
      }
    }]
  })
}

resource "aws_ecr_repository" "app" {
  name = local.name

  # IMMUTABLE means a tag can never be overwritten, which forces uniquely
  # tagged images. `make echo-image` already stamps the short git SHA, so this
  # costs nothing -- but pushing a fixed tag like `latest` twice will be
  # rejected, and that is the point.
  image_tag_mutability = "IMMUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "app" {
  repository = aws_ecr_repository.app.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep the last 10 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 10
      }
      action = { type = "expire" }
    }]
  })
}

# SES identity. Verifying a domain needs DNS you control; an email identity
# just needs you to click a link, which is the right size for learning.
resource "aws_ses_email_identity" "sender" {
  count = var.ses_sender_email == "" ? 0 : 1
  email = var.ses_sender_email
}

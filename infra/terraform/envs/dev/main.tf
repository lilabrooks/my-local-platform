# Dev environment on real AWS.
#
# Split by cost, deliberately:
#
#   ALWAYS CREATED -- serverless, pay-per-request, ~$0/month when idle.
#     S3, SNS, SQS, SES identity, ECR. Safe to leave standing.
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

  # Fill in after running the bootstrap stack, then `terraform init -migrate-state`.
  # Left commented so a fresh clone can `plan` with local state.
  #
  # backend "s3" {
  #   bucket       = "mlp-tfstate-<account-id>"
  #   key          = "envs/dev/terraform.tfstate"
  #   region       = "us-east-1"
  #   dynamodb_table = "mlp-tfstate-lock"
  #   encrypt      = true
  # }
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

resource "aws_sns_topic" "events" {
  name = "${local.name}-events"
}

resource "aws_sqs_queue" "events_dlq" {
  name                      = "${local.name}-events-dlq"
  message_retention_seconds = 1209600 # 14 days, the maximum
}

resource "aws_sqs_queue" "events" {
  name = "${local.name}-events"

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
  name                 = local.name
  image_tag_mutability = "MUTABLE"
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

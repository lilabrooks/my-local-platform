# One-time bootstrap: remote state for every other Terraform stack.
#
# Run this ONCE, then never again:
#   cd infra/terraform/bootstrap
#   AWS_PROFILE=aws-public-change-feed terraform init && terraform apply
#
# Cost: effectively zero. An empty S3 bucket and an on-demand DynamoDB table
# with no traffic bill under $0.01/month.
#
# This stack keeps its own state as a LOCAL file (bootstrap.tfstate), which is
# committed to .gitignore. That is deliberate -- it cannot store its state in
# the bucket it is creating.

terraform {
  required_version = ">= 1.9"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project   = "my-local-platform"
      ManagedBy = "terraform"
      Stack     = "bootstrap"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  # Bucket names are globally unique, so scope it to the account.
  state_bucket = "mlp-tfstate-${data.aws_caller_identity.current.account_id}"
}

# trivy:ignore:AWS-0132 SSE-S3 (AES256) is configured below. A customer-managed
# KMS key adds $1/month plus request charges; state files here are a single
# person's dev infrastructure, not regulated data.
resource "aws_s3_bucket" "state" {
  bucket = local.state_bucket

  # State files are the crown jewels: losing one orphans real infrastructure.
  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_versioning" "state" {
  bucket = aws_s3_bucket.state.id
  versioning_configuration {
    status = "Enabled"
  }
}

# AWS-0132 wants a customer-managed KMS key. Accepted, not fixed: SSE-S3
# (AES256) is configured below, and a CMK adds $1/month plus request charges to
# protect one person's dev state files.
#
# Note this directive sits on the encryption-configuration resource, not on the
# bucket -- that is where trivy anchors the finding, and a directive on the
# wrong resource does nothing at all.
#trivy:ignore:AWS-0132
resource "aws_s3_bucket_server_side_encryption_configuration" "state" {
  bucket = aws_s3_bucket.state.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "state" {
  bucket                  = aws_s3_bucket.state.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# AWS-0024 wants point-in-time recovery. Accepted, not fixed: PITR protects
# data worth restoring, and this table holds transient lock records that are
# meaningless the moment they are released. There is nothing a restore would
# recover.
#trivy:ignore:AWS-0024
resource "aws_dynamodb_table" "lock" {
  name         = "mlp-tfstate-lock"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }
}

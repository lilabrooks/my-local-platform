terraform {
  required_version = ">= 1.10, < 2.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
  backend "s3" {}
}

variable "operator_arn" {
  description = "Existing IAM user or role allowed to assume the two synthetic pilot roles."
  type        = string
  validation {
    condition     = can(regex("^arn:aws:iam::[0-9]{12}:(role|user)/[A-Za-z0-9+=,.@_/-]+$", var.operator_arn))
    error_message = "Supply one explicit commercial-partition IAM principal ARN."
  }
}
variable "router_zip" {
  description = "Path to the locally qualified Linux arm64 Lambda ZIP."
  type        = string
}
variable "fixture_bundle" {
  description = "Optional immutable Linux amd64 fixture tar and its expected SHA-256 for separately approved package staging."
  type        = object({ path = string, sha256 = string })
  default     = null
  validation {
    condition     = var.fixture_bundle == null ? true : can(regex("^[a-f0-9]{64}$", var.fixture_bundle.sha256))
    error_message = "Supply the SHA-256 from the qualified fixture bundle."
  }
}
variable "hourly" {
  description = "Separately approved private database session. Null runs only the signed identity stage."
  type        = object({ ami_id = string, executor_zip = string })
  default     = null
  validation {
    condition     = var.hourly == null ? true : can(regex("^ami-[a-f0-9]{17}$", var.hourly.ami_id))
    error_message = "Pin a regional ECS-optimized AL2023 x86_64 AMI."
  }
  validation {
    condition     = var.hourly == null || var.fixture_bundle != null
    error_message = "Stage the qualified fixture bundle before selecting an hourly session."
  }
}
provider "aws" {
  region              = "us-east-1"
  allowed_account_ids = [split(":", var.operator_arn)[4]]
  default_tags {
    tags = { Project = "my-local-platform", Environment = "dev", ManagedBy = "terraform", Ephemeral = "true", Application = "db-mcp" }
  }
}
locals {
  account = split(":", var.operator_arn)[4]
  name    = "mlp-db-mcp"
  lambda_trust = jsonencode({ Version = "2012-10-17", Statement = [{
    Effect = "Allow", Action = "sts:AssumeRole", Principal = { Service = "lambda.amazonaws.com" }
  }] })
  executor_arn = "arn:aws:lambda:us-east-1:${local.account}:function:${local.name}-executor"
}

resource "aws_iam_role" "caller" {
  for_each = toset(["reader", "observer"])
  name     = "${local.name}-${each.key}"
  assume_role_policy = jsonencode({ Version = "2012-10-17", Statement = [{
    Effect = "Allow", Action = "sts:AssumeRole", Principal = { AWS = var.operator_arn }
  }] })
}
resource "aws_iam_role_policy" "caller" {
  for_each = aws_iam_role.caller
  name     = "signed-url-only"
  role     = each.value.id
  policy = jsonencode({ Version = "2012-10-17", Statement = [
    { Effect = "Allow", Action = "lambda:InvokeFunctionUrl", Resource = aws_lambda_function.router.arn,
    Condition = { StringEquals = { "lambda:FunctionUrlAuthType" = "AWS_IAM" } } },
    { Effect = "Allow", Action = "lambda:InvokeFunction", Resource = aws_lambda_function.router.arn,
    Condition = { Bool = { "lambda:InvokedViaFunctionUrl" = "true" } } },
    { Effect = "Deny", Action = "lambda:InvokeFunction", Resource = [aws_lambda_function.router.arn, "${aws_lambda_function.router.arn}:*", local.executor_arn, "${local.executor_arn}:*"],
    Condition = { BoolIfExists = { "lambda:InvokedViaFunctionUrl" = "false" } } }
  ] })
}
resource "aws_iam_role" "router" {
  name               = "${local.name}-router"
  assume_role_policy = local.lambda_trust
}
resource "aws_cloudwatch_log_group" "router" {
  name              = "/aws/lambda/${local.name}-router"
  retention_in_days = 1
}
resource "aws_iam_role_policy" "router" {
  name = "router"
  role = aws_iam_role.router.id
  policy = jsonencode({ Version = "2012-10-17", Statement = concat([
    { Effect = "Allow", Action = ["logs:CreateLogStream", "logs:PutLogEvents"], Resource = "${aws_cloudwatch_log_group.router.arn}:*" }
    ], var.hourly == null ? [] : [
    { Effect = "Allow", Action = ["lambda:InvokeFunction"], Resource = local.executor_arn }
  ]) })
}
resource "aws_lambda_function" "router" {
  function_name    = "${local.name}-router"
  role             = aws_iam_role.router.arn
  filename         = var.router_zip
  source_code_hash = filebase64sha256(var.router_zip)
  runtime          = "provided.al2023"
  architectures    = ["arm64"]
  handler          = "bootstrap"
  timeout          = 25
  memory_size      = 128
  environment {
    variables = {
      PILOT_ACCOUNT    = local.account
      READER_ROLE_ID   = aws_iam_role.caller["reader"].unique_id
      OBSERVER_ROLE_ID = aws_iam_role.caller["observer"].unique_id
      EXECUTOR_ARN     = var.hourly == null ? "" : local.executor_arn
    }
  }
  depends_on = [aws_iam_role_policy.router]
}
resource "aws_lambda_function_url" "router" {
  function_name      = aws_lambda_function.router.function_name
  authorization_type = "AWS_IAM"
}
output "signed_url" {
  value       = aws_lambda_function_url.router.function_url
  description = "Use --aws-url with the reader and observer profiles."
}
output "caller_roles" {
  value = { for name, role in aws_iam_role.caller : name => role.arn }
}

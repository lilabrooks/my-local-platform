terraform {
  required_version = ">= 1.10, < 2.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}
variable "account" { type = string }
variable "reader_role" { type = string }
variable "ami_id" { type = string }
variable "availability_zone_id" { type = string }
variable "executor_zip" { type = string }
variable "bucket" { type = string }
variable "object_key" { type = string }
variable "version_id" { type = string }
variable "bundle_sha256" { type = string }

locals {
  name = "mlp-db-mcp"
  ip   = "10.73.0.10"
  eni_actions = ["ec2:CreateNetworkInterface", "ec2:DescribeNetworkInterfaces", "ec2:DescribeSubnets",
  "ec2:DeleteNetworkInterface", "ec2:AssignPrivateIpAddresses", "ec2:UnassignPrivateIpAddresses"]
  executor_arn = "arn:aws:lambda:us-east-1:${var.account}:function:${local.name}-executor"
}
data "aws_ami" "fixture" {
  owners = ["amazon"]
  filter {
    name   = "image-id"
    values = [var.ami_id]
  }
  filter {
    name   = "architecture"
    values = ["x86_64"]
  }
  filter {
    name   = "name"
    values = ["al2023-ami-ecs-hvm-*-x86_64"]
  }
}
data "aws_default_tags" "pilot" {}
resource "aws_vpc" "pilot" {
  cidr_block           = "10.73.0.0/24"
  enable_dns_hostnames = true
  enable_dns_support   = true
  tags                 = { Name = local.name }
}
resource "aws_subnet" "private" {
  vpc_id               = aws_vpc.pilot.id
  cidr_block           = "10.73.0.64/26"
  availability_zone_id = var.availability_zone_id
  tags                 = { Name = "${local.name}-private" }
}
resource "aws_subnet" "database" {
  availability_zone_id = var.availability_zone_id
  vpc_id               = aws_vpc.pilot.id
  cidr_block           = "10.73.0.0/27"
  tags                 = { Name = "${local.name}-mysql" }
}
resource "aws_security_group" "role" {
  for_each    = toset(["executor", "mysql", "secret-endpoint"])
  name        = "${local.name}-${each.key}"
  description = "Private db-mcp pilot ${each.key} connections"
  vpc_id      = aws_vpc.pilot.id
}
resource "aws_vpc_security_group_ingress_rule" "mysql" {
  security_group_id            = aws_security_group.role["mysql"].id
  referenced_security_group_id = aws_security_group.role["executor"].id
  ip_protocol                  = "tcp"
  from_port                    = 3306
  to_port                      = 3306
  description                  = "Verified MySQL TLS from the executor"
}
resource "aws_vpc_security_group_egress_rule" "mysql" {
  security_group_id            = aws_security_group.role["executor"].id
  referenced_security_group_id = aws_security_group.role["mysql"].id
  ip_protocol                  = "tcp"
  from_port                    = 3306
  to_port                      = 3306
  description                  = "Read the synthetic MySQL view"
}
resource "aws_vpc_security_group_ingress_rule" "secret" {
  for_each                     = toset(["mysql", "executor"])
  security_group_id            = aws_security_group.role["secret-endpoint"].id
  referenced_security_group_id = aws_security_group.role[each.key].id
  ip_protocol                  = "tcp"
  from_port                    = 443
  to_port                      = 443
  description                  = "Private Secrets Manager HTTPS"
}
resource "aws_vpc_security_group_egress_rule" "secret" {
  for_each                     = toset(["mysql", "executor"])
  security_group_id            = aws_security_group.role[each.key].id
  referenced_security_group_id = aws_security_group.role["secret-endpoint"].id
  ip_protocol                  = "tcp"
  from_port                    = 443
  to_port                      = 443
  description                  = "Private Secrets Manager HTTPS"
}
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.pilot.id
  service_name      = "com.amazonaws.us-east-1.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_vpc.pilot.default_route_table_id]
  policy = jsonencode({ Version = "2012-10-17", Statement = [{
    Effect    = "Allow", Principal = "*", Action = "s3:GetObjectVersion", Resource = "arn:aws:s3:::${var.bucket}/${var.object_key}",
    Condition = { ArnEquals = { "aws:PrincipalArn" = aws_iam_role.fixture.arn }, StringEquals = { "s3:VersionId" = var.version_id } }
  }] })
}
resource "aws_vpc_security_group_egress_rule" "s3" {
  security_group_id = aws_security_group.role["mysql"].id
  prefix_list_id    = aws_vpc_endpoint.s3.prefix_list_id
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
  description       = "Fetch the reviewed S3 fixture object version"
}
resource "aws_secretsmanager_secret" "reader" {
  name                    = "${local.name}-reader"
  recovery_window_in_days = 0
}
resource "aws_vpc_endpoint" "secret" {
  vpc_id              = aws_vpc.pilot.id
  service_name        = "com.amazonaws.us-east-1.secretsmanager"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [aws_subnet.private.id]
  private_dns_enabled = true
  security_group_ids  = [aws_security_group.role["secret-endpoint"].id]
  policy = jsonencode({ Version = "2012-10-17", Statement = [
    { Effect = "Allow", Principal = { AWS = aws_iam_role.fixture.arn }, Action = "secretsmanager:PutSecretValue", Resource = aws_secretsmanager_secret.reader.arn },
    { Effect = "Allow", Principal = { AWS = aws_iam_role.executor.arn }, Action = "secretsmanager:GetSecretValue", Resource = aws_secretsmanager_secret.reader.arn }
  ] })
}
resource "aws_iam_role" "fixture" {
  name = "${local.name}-fixture"
  assume_role_policy = jsonencode({ Version = "2012-10-17", Statement = [{
    Effect = "Allow", Action = "sts:AssumeRole", Principal = { Service = "ec2.amazonaws.com" }
  }] })
}
resource "aws_iam_role_policy" "fixture" {
  name = "seed"
  role = aws_iam_role.fixture.id
  policy = jsonencode({ Version = "2012-10-17", Statement = [
    { Effect = "Allow", Action = "s3:GetObjectVersion", Resource = "arn:aws:s3:::${var.bucket}/${var.object_key}", Condition = { StringEquals = { "s3:VersionId" = var.version_id } } },
    { Effect = "Allow", Action = "secretsmanager:PutSecretValue", Resource = aws_secretsmanager_secret.reader.arn }
  ] })
}
resource "aws_iam_instance_profile" "fixture" {
  name = "${local.name}-fixture"
  role = aws_iam_role.fixture.name
}
resource "aws_instance" "fixture" {
  ami                         = data.aws_ami.fixture.id
  instance_type               = "t3.small"
  subnet_id                   = aws_subnet.database.id
  private_ip                  = local.ip
  associate_public_ip_address = false
  iam_instance_profile        = aws_iam_instance_profile.fixture.name
  vpc_security_group_ids      = [aws_security_group.role["mysql"].id]
  volume_tags                 = merge(data.aws_default_tags.pilot.tags, { Name = "${local.name}-mysql" })
  metadata_options {
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }
  credit_specification { cpu_credits = "standard" }
  root_block_device {
    encrypted             = true
    volume_type           = "gp3"
    volume_size           = 30
    delete_on_termination = true
  }
  user_data_replace_on_change = true
  user_data = templatefile("${path.module}/seed.sh.tftpl", {
    bucket        = var.bucket, key = var.object_key, version_b64 = base64encode(var.version_id),
    bundle_sha256 = var.bundle_sha256, secret = aws_secretsmanager_secret.reader.arn, ip = local.ip
  })
  tags = { Name = "${local.name}-mysql" }
  depends_on = [aws_iam_role_policy.fixture, aws_vpc_security_group_egress_rule.s3, aws_vpc_endpoint.secret,
  aws_vpc_security_group_ingress_rule.secret, aws_vpc_security_group_egress_rule.secret]
}
resource "aws_iam_role" "executor" {
  name = "${local.name}-executor"
  assume_role_policy = jsonencode({ Version = "2012-10-17", Statement = [{
    Effect = "Allow", Action = "sts:AssumeRole", Principal = { Service = "lambda.amazonaws.com" }
  }] })
}
resource "aws_cloudwatch_log_group" "executor" {
  name              = "/aws/lambda/${local.name}-executor"
  retention_in_days = 1
}
resource "aws_iam_role_policy" "executor" {
  name = "private-reader"
  role = aws_iam_role.executor.id
  policy = jsonencode({ Version = "2012-10-17", Statement = [
    { Effect = "Allow", Action = "secretsmanager:GetSecretValue", Resource = aws_secretsmanager_secret.reader.arn },
    { Effect = "Allow", Action = ["logs:CreateLogStream", "logs:PutLogEvents"], Resource = "${aws_cloudwatch_log_group.executor.arn}:*" },
    { Effect = "Allow", Action = local.eni_actions, Resource = "*" },
    { Effect = "Deny", Action = local.eni_actions, Resource = "*", Condition = { ArnEquals = { "lambda:SourceFunctionArn" = local.executor_arn } } }
  ] })
}
resource "aws_lambda_function" "executor" {
  function_name    = "${local.name}-executor"
  role             = aws_iam_role.executor.arn
  filename         = var.executor_zip
  source_code_hash = filebase64sha256(var.executor_zip)
  runtime          = "provided.al2023"
  architectures    = ["arm64"]
  handler          = "bootstrap"
  timeout          = 20
  memory_size      = 256
  vpc_config {
    subnet_ids         = [aws_subnet.private.id]
    security_group_ids = [aws_security_group.role["executor"].id]
  }
  environment {
    variables = { PILOT_ACCOUNT = var.account, READER_ROLE_ID = var.reader_role, MYSQL_HOST = local.ip, READER_SECRET_ARN = aws_secretsmanager_secret.reader.arn }
  }
  depends_on = [aws_iam_role_policy.executor]
}

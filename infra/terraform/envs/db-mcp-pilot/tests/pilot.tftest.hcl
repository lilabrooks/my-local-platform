# Dummy files exercise the plan's hash checks. Packaging and real MySQL TLS
# are verified separately by db-mcp-pilot-test.
mock_provider "aws" {
  override_during = plan
  mock_resource "aws_iam_role" {
    defaults = { arn = "arn:aws:iam::${join("", ["0000", "0000", "0001"])}:role/fixture" }
  }
  mock_resource "aws_lambda_function" {
    defaults = { arn = "arn:aws:lambda:us-east-1:${join("", ["0000", "0000", "0001"])}:function:mlp-db-mcp-router" }
  }
  mock_resource "aws_secretsmanager_secret" {
    defaults = { arn = "arn:aws:secretsmanager:us-east-1:${join("", ["0000", "0000", "0001"])}:secret:mlp-db-mcp-reader-example" }
  }
  mock_resource "aws_cloudwatch_log_group" {
    defaults = { arn = "arn:aws:logs:us-east-1:${join("", ["0000", "0000", "0001"])}:log-group:/aws/lambda/test" }
  }
  mock_resource "aws_subnet" {
    defaults = { availability_zone = "us-east-1a" }
  }
  mock_resource "aws_s3_bucket" {
    defaults = { arn = "arn:aws:s3:::fixture-test", id = "fixture-test" }
  }
}
variables {
  operator_arn = "arn:aws:iam::${join("", ["0000", "0000", "0001"])}:role/operator"
  router_zip   = "main.tf"
}
run "identity_only" {
  command = plan
  assert {
    condition = (
      length(module.hourly) == 0 && length(aws_s3_bucket.fixture) == 0 &&
      aws_lambda_function_url.router.authorization_type == "AWS_IAM" &&
      aws_lambda_function.router.environment[0].variables.EXECUTOR_ARN == "" &&
      length(jsondecode(aws_iam_role_policy.router.policy).Statement) == 1
    )
    error_message = "The identity stage must have a signed router, no executor permission, bucket or hourly resources."
  }
  assert {
    condition = alltrue([for role in ["reader", "observer"] :
      jsondecode(aws_iam_role_policy.caller[role].policy).Statement[0].Condition.StringEquals["lambda:FunctionUrlAuthType"] == "AWS_IAM" &&
      jsondecode(aws_iam_role_policy.caller[role].policy).Statement[1].Condition.Bool["lambda:InvokedViaFunctionUrl"] == "true" &&
      jsondecode(aws_iam_role_policy.caller[role].policy).Statement[2].Effect == "Deny" &&
      jsondecode(aws_iam_role_policy.caller[role].policy).Statement[2].Condition.BoolIfExists["lambda:InvokedViaFunctionUrl"] == "false" &&
      jsondecode(aws_iam_role.caller[role].assume_role_policy).Statement[0].Principal.AWS == var.operator_arn
    ])
    error_message = "Both pilot roles require signed URL permissions, direct-invoke denial and the named trust principal."
  }
}
run "staging_without_hourly" {
  command = plan
  variables {
    fixture_bundle = { path = "main.tf", sha256 = filesha256("main.tf") }
  }
  assert {
    condition = (length(module.hourly) == 0 && length(aws_s3_object.fixture) == 1 &&
      aws_s3_bucket_versioning.fixture[0].versioning_configuration[0].status == "Enabled" &&
      jsondecode(aws_s3_bucket_policy.fixture[0].policy).Statement[1].Effect == "Deny" &&
      toset(jsondecode(aws_s3_bucket_policy.fixture[0].policy).Statement[1].Condition.ArnNotEquals["aws:PrincipalArn"]) == toset([var.operator_arn, "arn:aws:iam::${local.account}:role/${local.name}-fixture"]) &&
    aws_s3_bucket_public_access_block.fixture[0].block_public_policy)
    error_message = "Package staging needs one private versioned object and no hourly services."
  }
}
run "hourly_wiring" {
  command = plan
  variables {
    hourly         = { ami_id = "ami-0123456789abcdef0", executor_zip = "main.tf" }
    fixture_bundle = { path = "main.tf", sha256 = filesha256("main.tf") }
  }
  assert {
    condition = (length(module.hourly) == 1 &&
      jsondecode(aws_iam_role_policy.router.policy).Statement[1].Resource == local.executor_arn &&
    aws_lambda_function.router.environment[0].variables.EXECUTOR_ARN == local.executor_arn)
    error_message = "The router must invoke only the selected private executor."
  }
}
run "private_dependency_contract" {
  command = plan
  module { source = "./hourly" }
  override_resource {
    target          = aws_security_group.role["executor"]
    override_during = plan
    values          = { id = "sg-0123456789abcdef0" }
  }
  override_resource {
    target          = aws_security_group.role["mysql"]
    override_during = plan
    values          = { id = "sg-1123456789abcdef0" }
  }
  variables {
    account       = join("", ["0000", "0000", "0001"])
    reader_role   = "AROAEXAMPLE"
    ami_id        = "ami-0123456789abcdef0"
    executor_zip  = "main.tf"
    bucket        = "fixture-test"
    object_key    = "fixture/test.tar"
    version_id    = "immutable-test-version"
    bundle_sha256 = filesha256("main.tf")
  }
  assert {
    condition = (
      !aws_instance.fixture.associate_public_ip_address && aws_instance.fixture.private_ip == "10.73.0.10" &&
      aws_instance.fixture.root_block_device[0].encrypted && aws_instance.fixture.root_block_device[0].delete_on_termination &&
      aws_instance.fixture.metadata_options[0].http_tokens == "required" &&
      aws_vpc_endpoint.secret.private_dns_enabled && aws_vpc_endpoint.s3.vpc_endpoint_type == "Gateway" &&
      jsondecode(aws_vpc_endpoint.s3.policy).Statement[0].Principal == "*" &&
      jsondecode(aws_vpc_endpoint.s3.policy).Statement[0].Condition.ArnEquals["aws:PrincipalArn"] == aws_iam_role.fixture.arn &&
      jsondecode(aws_vpc_endpoint.s3.policy).Statement[0].Condition.StringEquals["s3:VersionId"] == var.version_id &&
      aws_vpc_security_group_ingress_rule.mysql.referenced_security_group_id == aws_security_group.role["executor"].id &&
      aws_vpc_security_group_ingress_rule.mysql.from_port == 3306 &&
      contains(aws_lambda_function.executor.vpc_config[0].security_group_ids, aws_security_group.role["executor"].id) &&
      aws_lambda_function.executor.environment[0].variables.MYSQL_HOST == aws_instance.fixture.private_ip &&
      jsondecode(aws_iam_role_policy.executor.policy).Statement[0].Action == "secretsmanager:GetSecretValue" &&
      jsondecode(aws_iam_role_policy.fixture.policy).Statement[1].Action == "secretsmanager:PutSecretValue" &&
      strcontains(aws_instance.fixture.user_data, base64encode(var.version_id)) &&
      strcontains(aws_instance.fixture.user_data, "After=cloud-final.service docker.service") &&
      strcontains(aws_instance.fixture.user_data, "systemctl start --no-block mlp-db-mcp-seed.service") &&
      strcontains(aws_instance.fixture.user_data, var.bundle_sha256)
    )
    error_message = "The hourly plan lost a private network, image-version, credential, TLS host or teardown prerequisite."
  }
}

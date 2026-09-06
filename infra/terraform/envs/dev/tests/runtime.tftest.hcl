mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = {
      account_id = join("", ["1234", "5678", "9012"])
      arn        = "arn:aws:iam::${join("", ["1234", "5678", "9012"])}:user/terraform-test"
      user_id    = "AIDATEST"
    }
  }

  mock_data "aws_partition" {
    defaults = {
      partition          = "aws"
      dns_suffix         = "amazonaws.com"
      reverse_dns_prefix = "com.amazonaws"
    }
  }

  mock_data "aws_region" {
    defaults = {
      region = "us-east-1"
    }
  }

  mock_data "aws_iam_session_context" {
    defaults = {
      issuer_arn = "arn:aws:iam::${join("", ["1234", "5678", "9012"])}:user/terraform-test"
    }
  }

  mock_data "aws_iam_policy_document" {
    defaults = {
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }

  mock_resource "aws_msk_serverless_cluster" {
    override_during = plan

    defaults = {
      arn                        = "arn:aws:kafka:us-east-1:${join("", ["1234", "5678", "9012"])}:cluster/mlp-dev/aaaaaaaa-bbbb-cccc-dddd-ffffffffffff-1"
      bootstrap_brokers_sasl_iam = "broker.example.invalid:9098"
      cluster_uuid               = "aaaaaaaa-bbbb-cccc-dddd-ffffffffffff-1"
      id                         = "test-msk-id"
      region                     = "us-east-1"
    }
  }

  mock_resource "aws_sns_topic" {
    defaults = {
      arn = "arn:aws:sns:us-east-1:${join("", ["1234", "5678", "9012"])}:mlp-dev-events"
    }
  }

  mock_resource "aws_sqs_queue" {
    defaults = {
      arn = "arn:aws:sqs:us-east-1:${join("", ["1234", "5678", "9012"])}:mlp-dev-events"
      id  = "https://sqs.us-east-1.amazonaws.com/${join("", ["1234", "5678", "9012"])}/mlp-dev-events"
      url = "https://sqs.us-east-1.amazonaws.com/${join("", ["1234", "5678", "9012"])}/mlp-dev-events"
    }
  }
}

run "default_plan_has_no_hourly_resources" {
  command = plan

  assert {
    condition = (
      length(module.vpc) == 0 &&
      length(module.eks) == 0 &&
      length(aws_db_instance.main) == 0 &&
      length(aws_msk_serverless_cluster.relay) == 0
    )
    error_message = "The default plan must not contain VPC, EKS, RDS, MSK, or NAT resources."
  }

  assert {
    condition     = length(aws_ecr_repository.image) == 2
    error_message = "The cheap tier must contain relay and sink ECR repositories."
  }

  assert {
    condition     = output.runtime_shape.hourly_enabled == false
    error_message = "The default runtime shape must report hourly resources disabled."
  }
}

run "live_runtime_matches_the_accepted_shape" {
  command = plan

  variables {
    budget_alert_email = "owner@example.invalid"
    eks_operator_cidr  = "192.0.2.10/32"
    enable_eks         = true
    enable_msk         = true
    enable_rds         = true
  }

  override_resource {
    target          = module.vpc[0].aws_subnet.private[0]
    override_during = plan
    values = {
      id = "subnet-private-a"
    }
  }

  override_resource {
    target          = module.vpc[0].aws_subnet.private[1]
    override_during = plan
    values = {
      id = "subnet-private-b"
    }
  }

  override_resource {
    target          = module.eks[0].aws_security_group.node[0]
    override_during = plan
    values = {
      id = "sg-eks-nodes"
    }
  }

  override_resource {
    target          = aws_security_group.msk[0]
    override_during = plan
    values = {
      id = "sg-msk"
    }
  }

  assert {
    condition = (
      length(module.vpc) == 1 &&
      length(module.eks) == 1 &&
      length(aws_db_instance.main) == 1 &&
      length(aws_msk_serverless_cluster.relay) == 1
    )
    error_message = "The enabled runtime must contain exactly one VPC, EKS cluster, RDS instance, and MSK cluster."
  }

  assert {
    condition = (
      length(aws_iam_role.pod_identity) == 3 &&
      length(aws_eks_pod_identity_association.runtime) == 3 &&
      aws_eks_pod_identity_association.runtime["relay-ingest"].namespace == "mlp" &&
      aws_eks_pod_identity_association.runtime["relay-ingest"].service_account == "relay-ingest" &&
      aws_eks_pod_identity_association.runtime["relay-deliver"].namespace == "mlp" &&
      aws_eks_pod_identity_association.runtime["relay-deliver"].service_account == "relay-deliver" &&
      aws_eks_pod_identity_association.runtime["keda-operator"].namespace == "keda" &&
      aws_eks_pod_identity_association.runtime["keda-operator"].service_account == "keda-operator"
    )
    error_message = "The enabled runtime must create three distinct Pod Identity roles and associations."
  }

  assert {
    condition = (
      length(aws_vpc_security_group_ingress_rule.msk_from_eks) == 1 &&
      aws_vpc_security_group_ingress_rule.msk_from_eks[0].from_port == 9098 &&
      aws_vpc_security_group_ingress_rule.msk_from_eks[0].to_port == 9098 &&
      aws_vpc_security_group_ingress_rule.msk_from_eks[0].referenced_security_group_id == "sg-eks-nodes" &&
      toset(aws_msk_serverless_cluster.relay[0].vpc_config[0].subnet_ids) == toset(["subnet-private-a", "subnet-private-b"]) &&
      toset(aws_msk_serverless_cluster.relay[0].vpc_config[0].security_group_ids) == toset(["sg-msk"])
    )
    error_message = "MSK must use the private subnets and admit IAM traffic only from the EKS node security group."
  }

  assert {
    condition = (
      terraform_data.runtime_contract[0].input.msk_cluster_arn == "arn:aws:kafka:us-east-1:${join("", ["1234", "5678", "9012"])}:cluster/mlp-dev/aaaaaaaa-bbbb-cccc-dddd-ffffffffffff-1" &&
      terraform_data.runtime_contract[0].input.delivery_topic_arn == "arn:aws:kafka:us-east-1:${join("", ["1234", "5678", "9012"])}:topic/mlp-dev/aaaaaaaa-bbbb-cccc-dddd-ffffffffffff-1/mlp.relay.deliveries" &&
      terraform_data.runtime_contract[0].input.dead_letter_topic_arn == "arn:aws:kafka:us-east-1:${join("", ["1234", "5678", "9012"])}:topic/mlp-dev/aaaaaaaa-bbbb-cccc-dddd-ffffffffffff-1/mlp.relay.deliveries.dlq" &&
      terraform_data.runtime_contract[0].input.delivery_group_arn == "arn:aws:kafka:us-east-1:${join("", ["1234", "5678", "9012"])}:group/mlp-dev/aaaaaaaa-bbbb-cccc-dddd-ffffffffffff-1/relay-deliver"
    )
    error_message = "The runtime contract must carry the exact cluster, topic, and group ARNs used by the IAM policies."
  }

  assert {
    condition = (
      output.runtime_shape.eks.kubernetes_version == "1.35" &&
      output.runtime_shape.eks.node_capacity_type == "SPOT" &&
      output.runtime_shape.eks.node_desired == 2 &&
      output.runtime_shape.eks.node_maximum == 3
    )
    error_message = "The EKS plan does not match ADR 0010's fixed node shape."
  }

  assert {
    condition = (
      output.runtime_shape.kafka.delivery_partitions == 12 &&
      output.runtime_shape.kafka.dead_letter_partitions == 1 &&
      output.runtime_shape.kafka.total_partitions == 13
    )
    error_message = "The Kafka plan does not match ADR 0010's 13-partition shape."
  }

  assert {
    condition     = output.runtime_shape.expected_hourly_usd <= output.runtime_shape.maximum_hourly_usd
    error_message = "The enabled runtime exceeds ADR 0010's hourly cost limit."
  }
}

run "eks_and_msk_without_rds_has_the_workload_boundary" {
  command = plan

  variables {
    budget_alert_email = "owner@example.invalid"
    eks_operator_cidr  = "192.0.2.10/32"
    enable_eks         = true
    enable_msk         = true
  }

  assert {
    condition = (
      length(aws_db_instance.main) == 0 &&
      length(aws_secretsmanager_secret.sink_signing_key) == 1 &&
      length(aws_eks_pod_identity_association.runtime) == 3 &&
      length(aws_vpc_security_group_ingress_rule.msk_from_eks) == 1
    )
    error_message = "EKS and MSK without RDS must retain the sink secret, identities, and private Kafka ingress boundary."
  }
}

run "hourly_flags_require_budget_configuration" {
  command = plan

  variables {
    enable_msk = true
  }

  expect_failures = [terraform_data.runtime_contract[0]]
}

run "eks_requires_an_operator_cidr" {
  command = plan

  variables {
    budget_alert_email = "owner@example.invalid"
    enable_eks         = true
  }

  expect_failures = [terraform_data.runtime_contract[0]]
}

run "partial_msk_apply_does_not_create_eks_or_rds" {
  command = apply

  variables {
    budget_alert_email = "owner@example.invalid"
    enable_msk         = true
  }

  assert {
    condition = (
      length(module.vpc) == 1 &&
      length(module.eks) == 0 &&
      length(aws_db_instance.main) == 0 &&
      length(aws_msk_serverless_cluster.relay) == 1 &&
      length(aws_iam_role.pod_identity) == 0
    )
    error_message = "The MSK-only partial apply must not create EKS, RDS, or Pod Identity resources."
  }
}

run "partial_msk_state_returns_to_the_cheap_tier" {
  command = apply

  variables {
    budget_alert_email = "owner@example.invalid"
  }

  assert {
    condition = (
      length(module.vpc) == 0 &&
      length(aws_msk_serverless_cluster.relay) == 0 &&
      length(terraform_data.runtime_contract) == 0
    )
    error_message = "Disabling the MSK flag must remove the partial runtime and retain only the cheap tier."
  }
}

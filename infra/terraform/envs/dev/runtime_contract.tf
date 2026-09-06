locals {
  runtime_budget_name = "${local.name}-live-runtime"

  delivery_topic         = "mlp.relay.deliveries"
  dead_letter_topic      = "mlp.relay.deliveries.dlq"
  delivery_group         = "relay-deliver"
  delivery_partitions    = 12
  dead_letter_partitions = 1

  eks_instance_type = "t3.medium"
  eks_node_capacity = "SPOT"
  eks_node_minimum  = 1
  eks_node_maximum  = 3
  eks_node_desired  = 2

  hourly_enabled = var.enable_eks || var.enable_msk || var.enable_rds

  # Rates checked for ADR 0010 on 2026-09-05. The gate deliberately prices
  # Spot nodes at the on-demand upper bound.
  expected_hourly_cost = (
    (var.enable_eks ? 0.100 + (local.eks_node_desired * 0.0416) + 0.045 + 0.005 : 0) +
    (var.enable_msk ? 0.750 + ((local.delivery_partitions + local.dead_letter_partitions) * 0.0015) : 0) +
    (var.enable_rds ? 0.016 + ((20 * 0.115) / 730) : 0)
  )
}

# Terraform enforces the parts that can be decided from configuration. The
# wrapper adds the two live checks Terraform cannot prove: the budget already
# exists, and AWS currently reports the EKS version in STANDARD_SUPPORT.
resource "terraform_data" "runtime_contract" {
  count = local.hourly_enabled ? 1 : 0

  input = {
    enable_eks            = var.enable_eks
    enable_msk            = var.enable_msk
    enable_rds            = var.enable_rds
    msk_cluster_arn       = local.msk_cluster_arn
    delivery_topic_arn    = local.delivery_topic_arn
    dead_letter_topic_arn = local.dead_letter_topic_arn
    delivery_group_arn    = local.delivery_group_arn
  }

  lifecycle {
    precondition {
      condition     = var.budget_alert_email != ""
      error_message = "Set budget_alert_email and apply the cheap tier before enabling hourly resources."
    }

    precondition {
      condition     = !var.enable_eks || var.eks_operator_cidr != ""
      error_message = "Set eks_operator_cidr to the operator's current IPv4 /32 before enabling EKS."
    }

    precondition {
      condition     = local.expected_hourly_cost <= 1.25
      error_message = "The configured runtime exceeds ADR 0010's $1.25/hour shape limit."
    }

    precondition {
      condition     = local.delivery_partitions + local.dead_letter_partitions <= 13
      error_message = "The configured runtime exceeds ADR 0010's 13-partition limit."
    }
  }
}

output "bucket" {
  description = "Artifacts bucket."
  value       = aws_s3_bucket.artifacts.id
}

output "topic_arn" {
  description = "SNS events topic."
  value       = aws_sns_topic.events.arn
}

output "queue_url" {
  description = "SQS queue subscribed to the events topic."
  value       = aws_sqs_queue.events.url
}

output "ecr_repository_urls" {
  description = "Service-specific repositories for immutable M4 images."
  value       = { for service, repository in aws_ecr_repository.image : service => repository.repository_url }
}

output "rds_endpoint" {
  description = "RDS endpoint, when enable_rds is true."
  value       = var.enable_rds ? aws_db_instance.main[0].endpoint : null
}

output "rds_master_secret_arn" {
  description = "ARN of the AWS-managed RDS master secret, when enable_rds is true. This is not the secret value."
  value       = var.enable_rds ? aws_db_instance.main[0].master_user_secret[0].secret_arn : null
}

output "sink_signing_secret_arn" {
  description = "ARN of the empty sink-signing secret container, when enable_eks is true. The value is staged outside Terraform."
  value       = var.enable_eks ? aws_secretsmanager_secret.sink_signing_key[0].arn : null
}

output "eks_cluster_name" {
  description = "EKS cluster name, when enable_eks is true."
  value       = var.enable_eks ? module.eks[0].cluster_name : null
}

output "kubeconfig_command" {
  description = "Run this to point kubectl at the cluster."
  value = var.enable_eks ? (
    "aws eks update-kubeconfig --name ${module.eks[0].cluster_name} --region ${var.region}"
  ) : null
}

output "msk_bootstrap_brokers" {
  description = "MSK Serverless IAM bootstrap brokers, when enable_msk is true."
  value       = var.enable_msk ? aws_msk_serverless_cluster.relay[0].bootstrap_brokers_sasl_iam : null
}

output "pod_identity_role_names" {
  description = "IAM role names keyed by the service account that consumes each Pod Identity."
  value       = { for name, role in aws_iam_role.pod_identity : name => role.name }
}

output "runtime_budget_name" {
  description = "Budget name checked by the guarded plan and apply."
  value       = local.runtime_budget_name
}

output "runtime_shape" {
  description = "Non-secret inputs used by the reviewed-plan resource and cost gate."
  value = {
    region              = var.region
    hourly_enabled      = local.hourly_enabled
    enable_eks          = var.enable_eks
    enable_msk          = var.enable_msk
    enable_rds          = var.enable_rds
    expected_hourly_usd = local.expected_hourly_cost
    maximum_hourly_usd  = 1.25
    eks = {
      kubernetes_version = var.eks_kubernetes_version
      node_capacity_type = local.eks_node_capacity
      node_desired       = local.eks_node_desired
      node_maximum       = local.eks_node_maximum
    }
    kafka = {
      delivery_topic         = local.delivery_topic
      delivery_partitions    = local.delivery_partitions
      dead_letter_topic      = local.dead_letter_topic
      dead_letter_partitions = local.dead_letter_partitions
      total_partitions       = local.delivery_partitions + local.dead_letter_partitions
    }
  }
}

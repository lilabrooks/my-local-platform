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

output "ecr_repository_url" {
  description = "Push container images here."
  value       = aws_ecr_repository.app.repository_url
}

output "rds_endpoint" {
  description = "RDS endpoint, when enable_rds is true."
  value       = var.enable_rds ? aws_db_instance.main[0].endpoint : null
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

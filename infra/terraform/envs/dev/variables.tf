variable "region" {
  description = "AWS region."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name, used as a resource name prefix."
  type        = string
  default     = "dev"
}

variable "ses_sender_email" {
  description = "Email address to verify with SES. Empty disables SES. You must click the verification link AWS sends."
  type        = string
  default     = ""
}

variable "enable_rds" {
  description = "Create an RDS Postgres instance. Roughly $15/month. Local Postgres in docker-compose is free."
  type        = bool
  default     = false
}

variable "enable_eks" {
  description = "Create an EKS cluster, node group and NAT gateway. Roughly $115/month. minikube is free."
  type        = bool
  default     = false
}

variable "enable_msk" {
  description = "Create an MSK Serverless cluster. Roughly $0.77/hour before throughput charges. Local Kafka is free."
  type        = bool
  default     = false
}

variable "eks_kubernetes_version" {
  description = "EKS Kubernetes version. The guarded plan and apply require STANDARD_SUPPORT."
  type        = string
  default     = "1.35"
}

variable "eks_operator_cidr" {
  description = "Single IPv4 /32 allowed to reach the public EKS API endpoint. Required when enable_eks is true."
  type        = string
  default     = ""

  validation {
    condition = (
      var.eks_operator_cidr == "" ||
      (can(cidrnetmask(var.eks_operator_cidr)) && endswith(var.eks_operator_cidr, "/32"))
    )
    error_message = "eks_operator_cidr must be empty or a valid IPv4 /32 CIDR."
  }
}

variable "budget_alert_email" {
  description = "Email subscriber for the $5 monthly forgotten-resource budget. Required before any hourly flag is enabled."
  type        = string
  default     = ""

  validation {
    condition     = var.budget_alert_email == "" || can(regex("^[^@[:space:]]+@[^@[:space:]]+$", var.budget_alert_email))
    error_message = "budget_alert_email must be empty or a syntactically valid email address."
  }
}

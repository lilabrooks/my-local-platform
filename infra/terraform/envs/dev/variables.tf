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
  description = "Create an EKS cluster, node group and NAT gateway. Roughly $110/month. minikube is free."
  type        = bool
  default     = false
}

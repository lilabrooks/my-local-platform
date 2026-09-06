# =============================================================================
# Expensive tier -- OFF by default. Each of these bills by the hour.
# =============================================================================

# VPC is only needed by RDS, EKS, and MSK, so it is created only when one is enabled.
# On its own a VPC is free; the NAT gateway inside it is not (~$32/month plus
# data processing), so single_nat_gateway is forced on and it is skipped
# entirely unless EKS needs egress.
module "vpc" {
  count   = local.needs_vpc ? 1 : 0
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 6.7"

  name = local.name
  cidr = "10.42.0.0/16"

  azs             = ["${var.region}a", "${var.region}b"]
  private_subnets = ["10.42.1.0/24", "10.42.2.0/24"]
  public_subnets  = ["10.42.101.0/24", "10.42.102.0/24"]

  # One NAT instead of one per AZ. Not highly available -- correct trade for dev.
  enable_nat_gateway = var.enable_eks
  single_nat_gateway = true

  enable_dns_hostnames = true
  enable_dns_support   = true

  # Required for EKS load balancer discovery.
  public_subnet_tags  = var.enable_eks ? { "kubernetes.io/role/elb" = "1" } : {}
  private_subnet_tags = var.enable_eks ? { "kubernetes.io/role/internal-elb" = "1" } : {}
}

locals {
  needs_vpc = var.enable_rds || var.enable_eks || var.enable_msk
}

# -----------------------------------------------------------------------------
# RDS -- roughly $15/month for db.t4g.micro single-AZ plus 20GB gp3.
# -----------------------------------------------------------------------------
resource "aws_db_subnet_group" "main" {
  count      = var.enable_rds ? 1 : 0
  name       = "${local.name}-db"
  subnet_ids = module.vpc[0].private_subnets
}

resource "aws_security_group" "rds" {
  count       = var.enable_rds ? 1 : 0
  name        = "${local.name}-rds"
  description = "Postgres access from inside the VPC only"
  vpc_id      = module.vpc[0].vpc_id

  ingress {
    description = "Postgres from within the VPC"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [module.vpc[0].vpc_cidr_block]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_db_instance" "main" {
  count = var.enable_rds ? 1 : 0

  identifier     = local.name
  engine         = "postgres"
  engine_version = "17.4"
  instance_class = "db.t4g.micro"

  allocated_storage = 20
  storage_type      = "gp3"
  storage_encrypted = true

  db_name  = "platform"
  username = "platform"
  # Managed by AWS in Secrets Manager -- no password in state or in git.
  manage_master_user_password = true

  db_subnet_group_name   = aws_db_subnet_group.main[0].name
  vpc_security_group_ids = [aws_security_group.rds[0].id]
  publicly_accessible    = false

  # Dev settings: destroy should be quick and not leave paid snapshots behind.
  skip_final_snapshot     = true
  backup_retention_period = 0
  deletion_protection     = false
  apply_immediately       = true
}

# The value is staged outside Terraform so it never enters state. Terraform
# owns only the short-lived secret container and its tags.
resource "aws_secretsmanager_secret" "sink_signing_key" {
  count = var.enable_eks ? 1 : 0

  name                    = "${local.name}/sink-signing-key"
  description             = "Controlled sink signing key for the M4 relay run"
  recovery_window_in_days = 0
}

# -----------------------------------------------------------------------------
# MSK Serverless -- roughly $0.77/hour for one cluster and 13 partitions.
# Topics are created by the staging command, not by runtime pods.
# -----------------------------------------------------------------------------
resource "aws_security_group" "msk" {
  count = var.enable_msk ? 1 : 0

  name        = "${local.name}-msk"
  description = "MSK IAM access from the EKS worker security group only"
  vpc_id      = module.vpc[0].vpc_id
}

resource "aws_vpc_security_group_ingress_rule" "msk_from_eks" {
  count = var.enable_msk && var.enable_eks ? 1 : 0

  security_group_id            = aws_security_group.msk[0].id
  referenced_security_group_id = module.eks[0].node_security_group_id
  description                  = "Kafka IAM from EKS workers"
  from_port                    = 9098
  to_port                      = 9098
  ip_protocol                  = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "msk_vpc" {
  count = var.enable_msk ? 1 : 0

  security_group_id = aws_security_group.msk[0].id
  description       = "MSK traffic inside the private VPC"
  cidr_ipv4         = module.vpc[0].vpc_cidr_block
  ip_protocol       = "-1"
}

resource "aws_msk_serverless_cluster" "relay" {
  count = var.enable_msk ? 1 : 0

  cluster_name = local.name

  vpc_config {
    subnet_ids         = module.vpc[0].private_subnets
    security_group_ids = [aws_security_group.msk[0].id]
  }

  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }
}

# -----------------------------------------------------------------------------
# EKS -- roughly $115/month: $73 control plane + 2x t3.medium + NAT gateway.
# For learning Kubernetes locally, `minikube start` costs nothing. Turn this on
# when you specifically want to learn EKS itself.
# -----------------------------------------------------------------------------
module "eks" {
  count   = var.enable_eks ? 1 : 0
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 21.25"

  name = local.name
  # MUST stay on a version in STANDARD support. A version in extended support
  # bills at $0.60/cluster/hour instead of $0.10 -- $438/month rather than $73,
  # applied automatically with no approval step. Check before changing:
  #   aws eks describe-cluster-versions \
  #     --query 'clusterVersions[?status==`STANDARD_SUPPORT`].clusterVersion'
  # 1.35 is in standard support until 2027-03-27.
  kubernetes_version = var.eks_kubernetes_version

  # Kubernetes 1.35 encrypts all API data with an AWS-owned key by default.
  # Avoid a customer-managed key that would remain pending deletion after the
  # short-lived cluster is destroyed.
  encryption_config = null

  vpc_id     = module.vpc[0].vpc_id
  subnet_ids = module.vpc[0].private_subnets

  endpoint_public_access       = true
  endpoint_public_access_cidrs = [var.eks_operator_cidr]

  addons = {
    eks-pod-identity-agent = {
      before_compute = true
    }
  }

  # Whoever runs `terraform apply` gets cluster admin, otherwise you apply
  # successfully and then cannot talk to your own cluster.
  enable_cluster_creator_admin_permissions = true

  eks_managed_node_groups = {
    default = {
      instance_types = [local.eks_instance_type]
      min_size       = local.eks_node_minimum
      max_size       = local.eks_node_maximum
      desired_size   = local.eks_node_desired
      capacity_type  = local.eks_node_capacity

      # IMDSv2 with hop limit 1 keeps node-role credentials out of ordinary
      # pods if a Pod Identity association is absent or broken.
      metadata_options = {
        http_endpoint               = "enabled"
        http_put_response_hop_limit = 1
        http_tokens                 = "required"
      }
    }
  }
}

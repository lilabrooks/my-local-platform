# =============================================================================
# Expensive tier -- OFF by default. Each of these bills by the hour.
# =============================================================================

# VPC is only needed by RDS and EKS, so it is created only when one is enabled.
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
  needs_vpc = var.enable_rds || var.enable_eks
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

# -----------------------------------------------------------------------------
# EKS -- roughly $110/month: $73 control plane + 2x t3.small + NAT gateway.
# For learning Kubernetes locally, `minikube start` costs nothing. Turn this on
# when you specifically want to learn EKS itself.
# -----------------------------------------------------------------------------
module "eks" {
  count   = var.enable_eks ? 1 : 0
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 21.25"

  name               = local.name
  kubernetes_version = "1.31"

  vpc_id     = module.vpc[0].vpc_id
  subnet_ids = module.vpc[0].private_subnets

  endpoint_public_access = true

  # Whoever runs `terraform apply` gets cluster admin, otherwise you apply
  # successfully and then cannot talk to your own cluster.
  enable_cluster_creator_admin_permissions = true

  eks_managed_node_groups = {
    default = {
      instance_types = ["t3.small"]
      min_size       = 1
      max_size       = 3
      desired_size   = 2
      capacity_type  = "SPOT" # ~70% cheaper; fine for dev
    }
  }
}

locals {
  create_pod_identities = var.enable_eks && var.enable_msk

  pod_identities = local.create_pod_identities ? {
    relay-ingest = {
      namespace       = "mlp"
      service_account = "relay-ingest"
    }
    relay-deliver = {
      namespace       = "mlp"
      service_account = "relay-deliver"
    }
    keda-operator = {
      namespace       = "keda"
      service_account = "keda-operator"
    }
  } : {}

  msk_cluster_arn = var.enable_msk ? aws_msk_serverless_cluster.relay[0].arn : null
  delivery_topic_arn = var.enable_msk ? format(
    "%s/%s",
    replace(aws_msk_serverless_cluster.relay[0].arn, ":cluster/", ":topic/"),
    local.delivery_topic,
  ) : null
  dead_letter_topic_arn = var.enable_msk ? format(
    "%s/%s",
    replace(aws_msk_serverless_cluster.relay[0].arn, ":cluster/", ":topic/"),
    local.dead_letter_topic,
  ) : null
  delivery_group_arn = var.enable_msk ? format(
    "%s/%s",
    replace(aws_msk_serverless_cluster.relay[0].arn, ":cluster/", ":group/"),
    local.delivery_group,
  ) : null
}

data "aws_iam_policy_document" "pod_identity_trust" {
  for_each = local.pod_identities

  statement {
    sid     = "AllowEksPodIdentity"
    effect  = "Allow"
    actions = ["sts:AssumeRole", "sts:TagSession"]

    principals {
      type        = "Service"
      identifiers = ["pods.eks.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/eks-cluster-arn"
      values   = [module.eks[0].cluster_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/kubernetes-namespace"
      values   = [each.value.namespace]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/kubernetes-service-account"
      values   = [each.value.service_account]
    }
  }
}

resource "aws_iam_role" "pod_identity" {
  for_each = local.pod_identities

  name               = "${local.name}-${each.key}"
  description        = "MSK authority for the ${each.value.service_account} EKS service account"
  assume_role_policy = data.aws_iam_policy_document.pod_identity_trust[each.key].json
}

data "aws_iam_policy_document" "relay_ingest" {
  count = local.create_pod_identities ? 1 : 0

  statement {
    sid       = "Connect"
    actions   = ["kafka-cluster:Connect"]
    resources = [local.msk_cluster_arn]
  }

  statement {
    sid = "ProduceDeliveries"
    actions = [
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:WriteData",
    ]
    resources = [local.delivery_topic_arn]
  }

  statement {
    sid       = "ReadDeliveryLag"
    actions   = ["kafka-cluster:DescribeGroup"]
    resources = [local.delivery_group_arn]
  }
}

data "aws_iam_policy_document" "relay_deliver" {
  count = local.create_pod_identities ? 1 : 0

  statement {
    sid       = "Connect"
    actions   = ["kafka-cluster:Connect"]
    resources = [local.msk_cluster_arn]
  }

  statement {
    sid = "ConsumeDeliveries"
    actions = [
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:ReadData",
    ]
    resources = [local.delivery_topic_arn]
  }

  statement {
    sid = "UseDeliveryGroup"
    actions = [
      "kafka-cluster:AlterGroup",
      "kafka-cluster:DescribeGroup",
    ]
    resources = [local.delivery_group_arn]
  }

  statement {
    sid = "ProduceDeadLetters"
    actions = [
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:WriteData",
    ]
    resources = [local.dead_letter_topic_arn]
  }
}

data "aws_iam_policy_document" "keda_operator" {
  count = local.create_pod_identities ? 1 : 0

  statement {
    sid       = "Connect"
    actions   = ["kafka-cluster:Connect"]
    resources = [local.msk_cluster_arn]
  }

  statement {
    sid       = "DescribeDeliveryTopic"
    actions   = ["kafka-cluster:DescribeTopic"]
    resources = [local.delivery_topic_arn]
  }

  statement {
    sid       = "ReadDeliveryLag"
    actions   = ["kafka-cluster:DescribeGroup"]
    resources = [local.delivery_group_arn]
  }
}

locals {
  pod_identity_policies = local.create_pod_identities ? {
    relay-ingest  = data.aws_iam_policy_document.relay_ingest[0].json
    relay-deliver = data.aws_iam_policy_document.relay_deliver[0].json
    keda-operator = data.aws_iam_policy_document.keda_operator[0].json
  } : {}
}

resource "aws_iam_policy" "pod_identity" {
  for_each = local.pod_identity_policies

  name        = "${local.name}-${each.key}"
  description = "Exact MSK permissions for ${each.key} during the M4 relay run"
  policy      = each.value
}

resource "aws_iam_role_policy_attachment" "pod_identity" {
  for_each = local.pod_identity_policies

  role       = aws_iam_role.pod_identity[each.key].name
  policy_arn = aws_iam_policy.pod_identity[each.key].arn
}

resource "aws_eks_pod_identity_association" "runtime" {
  for_each = local.pod_identities

  cluster_name    = module.eks[0].cluster_name
  namespace       = each.value.namespace
  service_account = each.value.service_account
  role_arn        = aws_iam_role.pod_identity[each.key].arn

  depends_on = [module.eks]
}

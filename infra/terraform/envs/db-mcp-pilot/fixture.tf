# Optional staging owns a versioned, private object. Its server TLS key must
# be readable only by the operator and the short-lived fixture instance.
resource "aws_s3_bucket" "fixture" {
  count         = var.fixture_bundle == null ? 0 : 1
  bucket_prefix = "${local.name}-fixture-"
  force_destroy = true
}
resource "aws_s3_bucket_public_access_block" "fixture" {
  count                   = length(aws_s3_bucket.fixture)
  bucket                  = aws_s3_bucket.fixture[0].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
resource "aws_s3_bucket_versioning" "fixture" {
  count  = length(aws_s3_bucket.fixture)
  bucket = aws_s3_bucket.fixture[0].id
  versioning_configuration { status = "Enabled" }
}
resource "aws_s3_bucket_server_side_encryption_configuration" "fixture" {
  count  = length(aws_s3_bucket.fixture)
  bucket = aws_s3_bucket.fixture[0].id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}
resource "aws_s3_bucket_policy" "fixture" {
  count  = length(aws_s3_bucket.fixture)
  bucket = aws_s3_bucket.fixture[0].id
  policy = jsonencode({ Version = "2012-10-17", Statement = [
    { Effect = "Deny", Principal = "*", Action = "s3:*", Resource = [aws_s3_bucket.fixture[0].arn, "${aws_s3_bucket.fixture[0].arn}/*"],
    Condition = { Bool = { "aws:SecureTransport" = "false" } } },
    { Effect = "Deny", Principal = "*", Action = ["s3:GetObject", "s3:GetObjectVersion"], Resource = "${aws_s3_bucket.fixture[0].arn}/*",
    Condition = { ArnNotEquals = { "aws:PrincipalArn" = [var.operator_arn, "arn:aws:iam::${local.account}:role/${local.name}-fixture"] } } }
  ] })
}
resource "aws_s3_object" "fixture" {
  count       = length(aws_s3_bucket.fixture)
  bucket      = aws_s3_bucket.fixture[0].id
  key         = "fixture/${var.fixture_bundle.sha256}.tar"
  source      = var.fixture_bundle.path
  source_hash = var.fixture_bundle.sha256
  lifecycle {
    precondition {
      condition     = filesha256(var.fixture_bundle.path) == var.fixture_bundle.sha256
      error_message = "The qualified fixture bundle changed."
    }
  }
  depends_on = [aws_s3_bucket_public_access_block.fixture, aws_s3_bucket_versioning.fixture, aws_s3_bucket_policy.fixture, aws_s3_bucket_server_side_encryption_configuration.fixture]
}
module "hourly" {
  count         = var.hourly == null ? 0 : 1
  source        = "./hourly"
  account       = local.account
  reader_role   = aws_iam_role.caller["reader"].unique_id
  ami_id        = var.hourly.ami_id
  executor_zip  = var.hourly.executor_zip
  bucket        = one(aws_s3_bucket.fixture[*].id)
  object_key    = one(aws_s3_object.fixture[*].key)
  version_id    = one(aws_s3_object.fixture[*].version_id)
  bundle_sha256 = try(var.fixture_bundle.sha256, null)
}

# tflint config.
#
# `terraform validate` checks that the configuration parses and type-checks.
# tflint catches the next layer: deprecated syntax, invalid instance types,
# missing required tags, and provider-specific mistakes that only surface at
# apply time.

plugin "terraform" {
  enabled = true
  preset  = "recommended"
}

plugin "aws" {
  enabled = true
  version = "0.44.0"
  source  = "github.com/terraform-linters/tflint-ruleset-aws"
}

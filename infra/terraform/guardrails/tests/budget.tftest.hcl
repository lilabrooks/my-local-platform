mock_provider "aws" {}

run "persistent_budget_matches_the_live_aws_contract" {
  command = plan

  variables {
    budget_alert_email = "owner@example.invalid"
  }

  assert {
    condition     = aws_budgets_budget.live_aws.name == "mlp-live-aws-monthly"
    error_message = "The guardrail budget name changed."
  }

  assert {
    condition = (
      aws_budgets_budget.live_aws.limit_amount == "5" &&
      aws_budgets_budget.live_aws.limit_unit == "USD" &&
      aws_budgets_budget.live_aws.time_unit == "MONTHLY"
    )
    error_message = "The guardrail budget must remain a $5 monthly USD budget."
  }

  assert {
    condition = (
      length(aws_budgets_budget.live_aws.cost_filter) == 1 &&
      one(aws_budgets_budget.live_aws.cost_filter).name == "TagKeyValue" &&
      toset(one(aws_budgets_budget.live_aws.cost_filter).values) == toset(["user:Project$my-local-platform"]) &&
      one(aws_budgets_budget.live_aws.cost_types).include_tax == false
    )
    error_message = "The budget must cover only Project=my-local-platform spending before tax."
  }

  assert {
    condition = toset([
      for item in aws_budgets_budget.live_aws.notification :
      "${item.notification_type}:${item.comparison_operator}:${item.threshold}:${item.threshold_type}"
      ]) == toset([
      "ACTUAL:GREATER_THAN:80:PERCENTAGE",
      "ACTUAL:GREATER_THAN:100:PERCENTAGE",
      "FORECASTED:GREATER_THAN:100:PERCENTAGE",
    ])
    error_message = "The guardrail budget notifications changed."
  }
}

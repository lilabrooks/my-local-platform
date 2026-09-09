output "budget_name" {
  description = "Persistent budget required by every hourly plan and apply."
  value       = aws_budgets_budget.live_aws.name
}

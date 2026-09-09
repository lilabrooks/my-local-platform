variable "region" {
  description = "AWS region used for authenticated guardrail operations."
  type        = string
  default     = "us-east-1"

  validation {
    condition     = var.region == "us-east-1"
    error_message = "The live AWS contract uses us-east-1."
  }
}

variable "budget_alert_email" {
  description = "Private email subscriber for the persistent $5 monthly budget."
  type        = string

  validation {
    condition     = can(regex("^[^@[:space:]]+@[^@[:space:]]+$", var.budget_alert_email))
    error_message = "budget_alert_email must be a syntactically valid email address."
  }
}

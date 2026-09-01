output "state_bucket" {
  description = "Put this in the backend block of every other stack."
  value       = aws_s3_bucket.state.id
}

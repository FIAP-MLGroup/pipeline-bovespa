output "function_name" {
  description = "Nome da função Lambda"
  value       = aws_lambda_function.glue_trigger.function_name
}

output "function_arn" {
  description = "ARN da função Lambda"
  value       = aws_lambda_function.glue_trigger.arn
}
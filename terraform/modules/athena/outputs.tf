output "workgroup_name" {
  description = "Nome do workgroup Athena"
  value       = aws_athena_workgroup.b3_workgroup.name
}

output "workgroup_arn" {
  description = "ARN do workgroup Athena"
  value       = aws_athena_workgroup.b3_workgroup.arn
}
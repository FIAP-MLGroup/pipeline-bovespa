output "job_name" {
  description = "Nome do job Glue"
  value       = aws_glue_job.etl_job.name
}

output "job_arn" {
  description = "ARN do job Glue"
  value       = aws_glue_job.etl_job.arn
}
output "raw_bucket_name" {
  description = "Nome do bucket para dados brutos"
  value       = aws_s3_bucket.raw.bucket
}

output "raw_bucket_arn" {
  description = "ARN do bucket para dados brutos"
  value       = aws_s3_bucket.raw.arn
}

output "refined_bucket_name" {
  description = "Nome do bucket para dados refinados"
  value       = aws_s3_bucket.refined.bucket
}

output "refined_bucket_arn" {
  description = "ARN do bucket para dados refinados"
  value       = aws_s3_bucket.refined.arn
}

output "athena_results_bucket_name" {
  description = "Nome do bucket para resultados do Athena"
  value       = aws_s3_bucket.athena_results.bucket
}

output "athena_results_bucket_arn" {
  description = "ARN do bucket para resultados do Athena"
  value       = aws_s3_bucket.athena_results.arn
}
output "raw_bucket_name" {
  description = "Nome do bucket S3 para dados brutos"
  value       = module.s3.raw_bucket_name
}

output "refined_bucket_name" {
  description = "Nome do bucket S3 para dados refinados"
  value       = module.s3.refined_bucket_name
}

output "lambda_function_name" {
  description = "Nome da função Lambda"
  value       = module.lambda.function_name
}

output "glue_job_name" {
  description = "Nome do job Glue"
  value       = module.glue.job_name
}

output "athena_workgroup_name" {
  description = "Nome do workgroup Athena"
  value       = module.athena.workgroup_name
}

output "scraper_command" {
  description = "Comando para executar o scraper"
  value       = "python -m scraper.b3_scraper --s3-bucket ${module.s3.raw_bucket_name}"
}
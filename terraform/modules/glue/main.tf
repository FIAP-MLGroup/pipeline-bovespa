resource "aws_s3_object" "glue_script" {
  bucket = var.raw_bucket_name
  key    = "scripts/glue-etl-job.py"
  source = "${path.module}/scripts/glue-etl-job.py"
  etag   = filemd5("${path.module}/scripts/glue-etl-job.py")
}

resource "aws_glue_job" "etl_job" {
  name         = "${var.project_name}-etl-job"
  role_arn     = var.lab_role_arn
  glue_version = "4.0"

  command {
    name            = "glueetl"
    script_location = "s3://${var.raw_bucket_name}/scripts/glue-etl-job.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-metrics"                   = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${var.raw_bucket_name}/spark-logs/"
    "--enable-job-insights"              = "true"
    "--enable-observability-metrics"     = "true"
    "--enable-glue-datacatalog"          = "true"
    "--RAW_BUCKET"                       = var.raw_bucket_name
    "--REFINED_BUCKET"                   = var.refined_bucket_name
  }

  max_retries       = 1
  timeout           = 30
  worker_type       = "G.1X"
  number_of_workers = 2

  depends_on = [aws_s3_object.glue_script]

  tags = {
    Name        = "${var.project_name}-etl-job"
    Environment = "dev"
    Purpose     = "ETL processing for B3 data"
  }
}

resource "aws_cloudwatch_log_group" "glue_job_logs" {
  name              = "/aws-glue/jobs/${aws_glue_job.etl_job.name}"
  retention_in_days = 7

  tags = {
    Name        = "${var.project_name}-glue-logs"
    Environment = "dev"
  }
}
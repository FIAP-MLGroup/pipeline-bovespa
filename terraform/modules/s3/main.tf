resource "random_string" "bucket_suffix" {
  length  = 8
  lower   = true
  upper   = false
  numeric = true
  special = false
}

resource "aws_s3_bucket" "raw" {
  bucket = "${var.project_name}-raw-${random_string.bucket_suffix.result}"

  tags = {
    Name        = "${var.project_name}-raw-bucket"
    Environment = "dev"
    Purpose     = "Raw data storage"
  }
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket" "refined" {
  bucket = "${var.project_name}-refined-${random_string.bucket_suffix.result}"

  tags = {
    Name        = "${var.project_name}-refined-bucket"
    Environment = "dev"
    Purpose     = "Refined data storage"
  }
}

resource "aws_s3_bucket_versioning" "refined" {
  bucket = aws_s3_bucket.refined.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "refined" {
  bucket = aws_s3_bucket.refined.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket" "athena_results" {
  bucket = "${var.project_name}-athena-results-${random_string.bucket_suffix.result}"

  tags = {
    Name        = "${var.project_name}-athena-results-bucket"
    Environment = "dev"
    Purpose     = "Athena query results"
  }
}

resource "aws_s3_bucket_versioning" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
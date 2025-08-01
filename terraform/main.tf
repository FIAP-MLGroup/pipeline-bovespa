terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

data "aws_caller_identity" "current" {}

module "s3" {
  source = "./modules/s3"
  
  project_name = var.project_name
  region       = var.region
}

module "lambda" {
  source = "./modules/lambda"
  
  project_name    = var.project_name
  region          = var.region
  lab_role_arn    = var.lab_role_arn
  glue_job_name   = module.glue.job_name
  raw_bucket_name = module.s3.raw_bucket_name
}

module "glue" {
  source = "./modules/glue"
  
  project_name        = var.project_name
  region              = var.region
  lab_role_arn        = var.lab_role_arn
  raw_bucket_name     = module.s3.raw_bucket_name
  refined_bucket_name = module.s3.refined_bucket_name
}

module "athena" {
  source = "./modules/athena"
  
  project_name            = var.project_name
  region                  = var.region
  athena_results_bucket   = module.s3.athena_results_bucket_name
}
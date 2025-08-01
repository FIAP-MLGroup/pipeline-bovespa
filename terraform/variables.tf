variable "region" {
  description = "AWS region"
  type        = string
  default     = "sa-east-1"
}

variable "project_name" {
  description = "Project name prefix"
  type        = string
  default     = "b3-tech-challenge"
}

variable "lab_role_arn" {
  description = "ARN of the existing LabRole"
  type        = string
}
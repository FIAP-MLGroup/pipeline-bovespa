variable "project_name" {
  description = "Nome do projeto"
  type        = string
}

variable "region" {
  description = "Região AWS"
  type        = string
}

variable "lab_role_arn" {
  description = "ARN da role do laboratório"
  type        = string
}

variable "raw_bucket_name" {
  description = "Nome do bucket de dados brutos"
  type        = string
}

variable "refined_bucket_name" {
  description = "Nome do bucket de dados refinados"
  type        = string
}
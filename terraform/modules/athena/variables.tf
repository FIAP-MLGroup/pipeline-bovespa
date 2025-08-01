variable "project_name" {
  description = "Nome do projeto"
  type        = string
}

variable "region" {
  description = "Região AWS"
  type        = string
}

variable "athena_results_bucket" {
  description = "Nome do bucket para resultados do Athena"
  type        = string
}
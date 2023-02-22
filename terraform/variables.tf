variable "name_bucket" {
  type        = string
  description = "Nome do bucket"
}

variable "versioning_bucket" {
  type        = string
  description = "Diz se o versionamento do bucket estará habilitado"
}

variable "files_bucket" {
  type        = string
  description = "Pasta de onde os scripts python serão obtidos"
  default     = "../python/python_aws"
}

variable "files_data" {
  type        = string
  description = "Pasta de onde os dados serão obtidos"
  default     = "../data/dados_brutos"
}

variable "files_bash" {
  type        = string
  description = "Pasta de onde os scripts bash serão obtidos"
  default     = "./scripts"
}

variable "name_emr" {
  type        = string
  description = "Nome do cluster EMR"
}

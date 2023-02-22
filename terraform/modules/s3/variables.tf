
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
  description = "Diz se o versionamento do bucket estará habilitado"
}

variable "files_data" {
  type        = string
  description = "Pasta de onde os dados serão obtidos"
}

variable "files_bash" {
  type        = string
  description = "Pasta de onde os scripts bash serão obtidos"
}
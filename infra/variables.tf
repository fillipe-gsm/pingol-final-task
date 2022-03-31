variable "student_code" {
  description = "Código de matrícula do aluno"
  type        = string
  default     = "138979"
}

variable "location" {
  description = "Região de uso dos recursos"
  type        = string
  default     = "brazilsouth"
}


variable "synapse_sql_login" {
  description = "Login do SQL do Workspace do Synapse Analytics"
  type        = string
  default     = "usersynapse"
}

variable "synapse_sql_password" {
  description = "Senha do SQL do Workspace do Synapse Analytics"
  type        = string
  default     = "#User-Synapse789!"
}

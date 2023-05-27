variable "project" {
  type        = string
  description = "ID Google project"
}

variable "environment" {
  type        = string
  description = "Project environment"
}

variable "bucket-landing-zone" {
  type        = string
  description = "Landing zone bucket name"
}

variable "bucket-curated-zone" {
  type        = string
  description = "Curated zone bucket name"
}

variable "bucket-presentation-zone" {
  type        = string
  description = "Presentation zone bucket name"
}

variable "default-bucket" {
  type        = string
  description = "Default bucket name"
  default     = ""
}

variable "region" {
  type        = string
  description = "Region Google project"
  default     = "us-central1"
}

variable "zone" {
  type        = string
  description = "Region zone Google project"
  default     = "us-central1-c"
}

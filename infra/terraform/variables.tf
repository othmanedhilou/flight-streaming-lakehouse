variable "project" {
  type    = string
  default = "flights"
}

variable "env" {
  type    = string
  default = "dev"
}

variable "location" {
  type    = string
  default = "francecentral"
}

variable "deploy_producer" {
  description = "Set to true once the producer image has been pushed to the registry"
  type        = bool
  default     = false
}

variable "producer_image_tag" {
  type    = string
  default = "latest"
}

variable "poll_interval_seconds" {
  type    = number
  default = 60
}

variable "opensky_client_id" {
  type      = string
  default   = ""
  sensitive = true
}

variable "opensky_client_secret" {
  type      = string
  default   = ""
  sensitive = true
}

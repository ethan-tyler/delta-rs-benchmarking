variable "api_allowed_cidrs" {
  description = "CIDRs allowed for Vultr API key ACLs."
  type        = list(string)
  default     = []

  validation {
    condition = alltrue([
      for cidr in var.api_allowed_cidrs :
      cidr != "0.0.0.0/0" && cidr != "::/0"
    ])
    error_message = "api_allowed_cidrs must not contain 0.0.0.0/0 or ::/0."
  }
}

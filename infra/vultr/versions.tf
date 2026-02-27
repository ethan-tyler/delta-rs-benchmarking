terraform {
  required_version = ">= 1.7.0"

  required_providers {
    vultr = {
      source  = "vultr/vultr"
      version = "~> 2.19"
    }
  }
}

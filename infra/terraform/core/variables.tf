variable "region" {
  type    = string
  default = "eu-north-1"
}

variable "alert_email" {
  type        = string
  description = "Where collection alarms go (SNS asks you to confirm the subscription by email)."
}

variable "collector_enabled" {
  type        = bool
  default     = false
  description = <<-EOT
    Turns the poller and static-watcher schedules on. Only one poller may use the NTA key
    (ADR-0005): stop the laptop poller first.
  EOT
}

variable "poll_offset_s" {
  type    = number
  default = 25
  # EventBridge Scheduler fires warm invocations about 6 s into the minute, but the first run
  # after enabling arrived at 14.8 s and its minute was skipped (18 Nov). An invocation that
  # starts after this second skips its minute rather than call late; 25 s leaves ~19 s of slack
  # and keeps Lambda use (~10-25 s at 256 MB per minute) inside the always-free allowance.
  description = "Second of each minute the poller calls at."
}

variable "nta_key_parameter" {
  type        = string
  default     = "/ghostbus/nta-api-key"
  description = <<-EOT
    SSM SecureString holding the NTA key. Created outside Terraform so the key never enters
    Terraform state (see docs/05-aws-setup.md).
  EOT
}

variable "log_retention_days" {
  type    = number
  default = 14
}

variable "batch_enabled" {
  type        = bool
  default     = false
  description = "Turns the nightly Glue triggers on (timetable load, then reconcile)."
}

variable "glue_workers" {
  type        = number
  default     = 2
  description = "G.1X workers per Glue job (2 is the minimum). Each costs $0.29 an hour on Flex."
}

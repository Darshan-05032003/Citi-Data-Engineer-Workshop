# Talent pipeline: let the EKS job read its HR source from Aurora PostgreSQL.
# Default SOURCE_MODE stays "files", so applying this file alone does not change the job's behaviour.

variable "talent_source_mode" {
  type        = string
  default     = "files"
  description = "Where the talent-pipeline job reads the 7 HR tables from: files (S3 CSVs) or postgres (Aurora via JDBC)."

  validation {
    condition     = contains(["files", "postgres"], var.talent_source_mode)
    error_message = "talent_source_mode must be \"files\" or \"postgres\"."
  }
}

# EKS managed nodes carry only the EKS cluster security group, while Aurora's security group accepts
# traffic only from its own members. Allow PostgreSQL (TCP 5432) and nothing else, from the cluster SG only.
resource "aws_vpc_security_group_ingress_rule" "talent_eks_to_aurora" {
  count = data.aws_caller_identity.this.id != "000000000000" && var.aws_eks_enabled ? length(data.aws_security_groups.this.ids) : 0

  security_group_id            = data.aws_security_groups.this.ids[count.index]
  referenced_security_group_id = aws_eks_cluster.this[0].vpc_config[0].cluster_security_group_id
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
  description                  = "talent-pipeline EKS job -> Aurora PostgreSQL (5432 only)"
  tags                         = local.app_tags
}

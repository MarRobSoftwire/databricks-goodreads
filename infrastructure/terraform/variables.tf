variable "subscription_id" {
  description = "Azure subscription ID"
  type        = string
  default     = "6dac3c53-845b-4aec-b4b7-2db823a9b249"
}

variable "resource_group_name" {
  description = "Name of the existing Azure resource group"
  type        = string
  default     = "marrob-databricks-training"
}

variable "workspace_name" {
  description = "Name of the Databricks workspace"
  type        = string
  default     = "marrob-databricks-workspace"
}

variable "tags" {
  description = "Tags applied to all resources"
  type        = map(string)
  default     = {}
}

# -----------------------------------------------------------------------
# Unity Catalog variables
# -----------------------------------------------------------------------

variable "databricks_account_id" {
  description = "Databricks account ID (found at accounts.azuredatabricks.net > Settings)"
  type        = string
}

variable "uc_storage_account_name" {
  description = "Globally unique name for the ADLS Gen2 storage account used as the metastore root (3-24 lowercase alphanumeric chars)"
  type        = string
  default     = "marrobdbucstorage"
}

variable "uc_metastore_name" {
  description = "Name for the Unity Catalog metastore"
  type        = string
  default     = "goodreads-metastore"
}

variable "uc_catalog_name" {
  description = "Name for the top-level Unity Catalog catalog"
  type        = string
  default     = "goodreads"
}

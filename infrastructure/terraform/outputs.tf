output "workspace_url" {
  description = "Databricks workspace URL — use this as the host value in databricks.yml targets"
  value       = "https://${azurerm_databricks_workspace.databricks_workspace.workspace_url}"
}

output "workspace_id" {
  description = "Azure resource ID of the Databricks workspace"
  value       = azurerm_databricks_workspace.databricks_workspace.id
}

output "metastore_id" {
  description = "Unity Catalog metastore ID"
  value       = databricks_metastore.this.id
}

output "uc_catalog_name" {
  description = "Name of the Unity Catalog catalog — update databricks.yml table references accordingly"
  value       = databricks_catalog.goodreads.name
}

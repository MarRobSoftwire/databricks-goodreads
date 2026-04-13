# -----------------------------------------------------------------------
# Azure prerequisites for Unity Catalog
# -----------------------------------------------------------------------

# Managed identity that the metastore uses to access ADLS storage
resource "azurerm_databricks_access_connector" "uc" {
  name                = "${var.workspace_name}-uc-connector"
  resource_group_name = data.azurerm_resource_group.main.name
  location            = data.azurerm_resource_group.main.location

  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
}

# ADLS Gen2 storage account for the metastore root
# NOTE: storage account names must be globally unique, 3-24 lowercase alphanumeric chars
resource "azurerm_storage_account" "uc_metastore" {
  name                     = var.uc_storage_account_name
  resource_group_name      = data.azurerm_resource_group.main.name
  location                 = data.azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true # required for ADLS Gen2

  tags = var.tags
}

resource "azurerm_storage_container" "uc_metastore" {
  name                  = "metastore"
  storage_account_id    = azurerm_storage_account.uc_metastore.id
  container_access_type = "private"
}

# Grant the access connector's managed identity read/write on the storage container
resource "azurerm_role_assignment" "uc_storage" {
  scope                = azurerm_storage_account.uc_metastore.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.uc.identity[0].principal_id
}

# -----------------------------------------------------------------------
# Unity Catalog metastore (account-level resource)
# -----------------------------------------------------------------------

resource "databricks_metastore" "this" {
  provider = databricks.accounts

  name = var.uc_metastore_name
  storage_root = format(
    "abfss://%s@%s.dfs.core.windows.net/",
    azurerm_storage_container.uc_metastore.name,
    azurerm_storage_account.uc_metastore.name,
  )
  region        = data.azurerm_resource_group.main.location
  force_destroy = true
}

# Link the access connector to the metastore as its default data access credential
resource "databricks_metastore_data_access" "this" {
  provider     = databricks.accounts
  metastore_id = databricks_metastore.this.id
  name         = "uc-managed-identity"

  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.uc.id
  }

  is_default = true
}

# Assign the metastore to the workspace
resource "databricks_metastore_assignment" "this" {
  provider     = databricks.accounts
  metastore_id = databricks_metastore.this.id
  workspace_id = azurerm_databricks_workspace.databricks_workspace.workspace_id

  depends_on = [databricks_metastore_data_access.this]
}

# -----------------------------------------------------------------------
# Unity Catalog objects (workspace-level)
# Matches the 3-level namespace used in databricks.yml:
#   <uc_catalog_name>.goodreads.gold_pages_per_day
# -----------------------------------------------------------------------

resource "databricks_catalog" "goodreads" {
  name    = var.uc_catalog_name
  comment = "Managed by Terraform"

  depends_on = [databricks_metastore_assignment.this]
}

resource "databricks_schema" "goodreads" {
  catalog_name = databricks_catalog.goodreads.name
  name         = "goodreads"
  comment      = "Managed by Terraform"
}

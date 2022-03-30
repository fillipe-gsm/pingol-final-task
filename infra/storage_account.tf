resource "azurerm_storage_account" "storage_account" {
  name                     = "contafinal${var.student_code}"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "GRS"
  account_kind             = "StorageV2"
  allow_blob_public_access = "true"
  is_hns_enabled           = "true"
  network_rules {
    default_action = "Allow"
  }
  tags       = azurerm_resource_group.rg.tags
  depends_on = [azurerm_resource_group.rg]
}

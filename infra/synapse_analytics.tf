resource "azurerm_synapse_workspace" "synapse_analytics" {
  name                                 = "dwsynapse${var.student_code}"
  resource_group_name                  = azurerm_resource_group.rg.name
  location                             = azurerm_resource_group.rg.location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.datalake_gen2.id
  sql_administrator_login              = var.synapse_sql_login
  sql_administrator_login_password     = var.synapse_sql_password

  # Descomente o código caso use o provider azurerm na versão acima da 3.0.0
  /* identity = {
        type = "SystemAssigned"
    } */

  tags = azurerm_resource_group.rg.tags
  depends_on = [
    azurerm_resource_group.rg
  ]
}

resource "azurerm_synapse_firewall_rule" "synapse_firewall" {
  name                 = "AllowAll"
  synapse_workspace_id = azurerm_synapse_workspace.synapse_analytics.id
  start_ip_address     = "0.0.0.0"
  end_ip_address       = "255.255.255.255"
  depends_on = [
    azurerm_synapse_workspace.synapse_analytics
  ]
}

resource "azurerm_role_assignment" "synapse-datalake-permission" {
  scope                = azurerm_storage_account.storage_account.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_synapse_workspace.synapse_analytics.identity.0.principal_id
  depends_on = [
    azurerm_synapse_firewall_rule.synapse_firewall,
    azurerm_storage_account.storage_account
  ]
}

resource "azurerm_storage_data_lake_gen2_filesystem" "datalake_gen2" {
  name               = "datalakegen2${var.student_code}"
  storage_account_id = azurerm_storage_account.storage_account.id
}

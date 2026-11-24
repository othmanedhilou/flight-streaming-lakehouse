output "eventhub_namespace" {
  value = azurerm_eventhub_namespace.ns.name
}

output "databricks_workspace_url" {
  value = "https://${azurerm_databricks_workspace.dbw.workspace_url}"
}

output "key_vault_uri" {
  value = azurerm_key_vault.kv.vault_uri
}

output "key_vault_id" {
  value = azurerm_key_vault.kv.id
}

output "acr_login_server" {
  value = azurerm_container_registry.acr.login_server
}

output "acr_name" {
  value = azurerm_container_registry.acr.name
}

output "checkpoints_path" {
  value = "abfss://${azurerm_storage_container.lakehouse.name}@${azurerm_storage_account.lake.name}.dfs.core.windows.net/_checkpoints"
}

output "producer_send_connection_string" {
  value     = azurerm_eventhub_authorization_rule.send.primary_connection_string
  sensitive = true
}

terraform {
  required_version = ">= 1.6"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy = true
    }
  }
}

data "azurerm_client_config" "current" {}

locals {
  name = "${var.project}-${var.env}"
  tags = { project = var.project, env = var.env }
}

resource "azurerm_resource_group" "rg" {
  name     = "rg-${local.name}"
  location = var.location
  tags     = local.tags
}

# ---------- ingestion ----------

# Standard tier is the lowest tier that exposes the Kafka endpoint
resource "azurerm_eventhub_namespace" "ns" {
  name                = "evhns-${local.name}"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  sku                 = "Standard"
  capacity            = 1
  tags                = local.tags
}

resource "azurerm_eventhub" "flights" {
  name              = "flights"
  namespace_id      = azurerm_eventhub_namespace.ns.id
  partition_count   = 4
  message_retention = 1
}

resource "azurerm_eventhub_consumer_group" "databricks" {
  name                = "databricks"
  namespace_name      = azurerm_eventhub_namespace.ns.name
  eventhub_name       = azurerm_eventhub.flights.name
  resource_group_name = azurerm_resource_group.rg.name
}

resource "azurerm_eventhub_authorization_rule" "send" {
  name                = "producer-send"
  namespace_name      = azurerm_eventhub_namespace.ns.name
  eventhub_name       = azurerm_eventhub.flights.name
  resource_group_name = azurerm_resource_group.rg.name
  send                = true
}

resource "azurerm_eventhub_authorization_rule" "listen" {
  name                = "databricks-listen"
  namespace_name      = azurerm_eventhub_namespace.ns.name
  eventhub_name       = azurerm_eventhub.flights.name
  resource_group_name = azurerm_resource_group.rg.name
  listen              = true
}

# ---------- storage ----------

resource "azurerm_storage_account" "lake" {
  name                     = "st${var.project}lake${var.env}"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true
  min_tls_version          = "TLS1_2"
  tags                     = local.tags
}

resource "azurerm_storage_container" "lakehouse" {
  name                  = "lakehouse"
  storage_account_id    = azurerm_storage_account.lake.id
  container_access_type = "private"
}

# ---------- secrets ----------

resource "azurerm_key_vault" "kv" {
  name                      = "kv-${local.name}"
  location                  = azurerm_resource_group.rg.location
  resource_group_name       = azurerm_resource_group.rg.name
  tenant_id                 = data.azurerm_client_config.current.tenant_id
  sku_name                  = "standard"
  enable_rbac_authorization = true
  tags                      = local.tags
}

resource "azurerm_role_assignment" "kv_admin" {
  scope                = azurerm_key_vault.kv.id
  role_definition_name = "Key Vault Secrets Officer"
  principal_id         = data.azurerm_client_config.current.object_id
}

resource "azurerm_key_vault_secret" "listen" {
  name         = "eventhub-listen-connection-string"
  value        = azurerm_eventhub_authorization_rule.listen.primary_connection_string
  key_vault_id = azurerm_key_vault.kv.id
  depends_on   = [azurerm_role_assignment.kv_admin]
}

# ---------- processing ----------

resource "azurerm_databricks_workspace" "dbw" {
  name                = "dbw-${local.name}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "premium"
  tags                = local.tags
}

resource "azurerm_databricks_access_connector" "uc" {
  name                = "dbac-${local.name}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_role_assignment" "uc_blob" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.uc.identity[0].principal_id
}

# ---------- producer ----------

resource "azurerm_container_registry" "acr" {
  name                = "acr${var.project}${var.env}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "Basic"
  admin_enabled       = true
  tags                = local.tags
}

resource "azurerm_container_group" "producer" {
  count               = var.deploy_producer ? 1 : 0
  name                = "aci-${local.name}-producer"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  os_type             = "Linux"
  restart_policy      = "Always"
  ip_address_type     = "None"

  image_registry_credential {
    server   = azurerm_container_registry.acr.login_server
    username = azurerm_container_registry.acr.admin_username
    password = azurerm_container_registry.acr.admin_password
  }

  container {
    name   = "producer"
    image  = "${azurerm_container_registry.acr.login_server}/flight-producer:${var.producer_image_tag}"
    cpu    = 0.5
    memory = 0.5

    commands = ["python", "-m", "flightstream.producer", "--sink", "eventhub", "--interval", tostring(var.poll_interval_seconds)]

    environment_variables = {
      EVENTHUB_NAME = azurerm_eventhub.flights.name
    }
    secure_environment_variables = {
      EVENTHUB_CONNECTION_STRING = azurerm_eventhub_authorization_rule.send.primary_connection_string
      OPENSKY_CLIENT_ID          = var.opensky_client_id
      OPENSKY_CLIENT_SECRET      = var.opensky_client_secret
    }
  }

  tags = local.tags
}

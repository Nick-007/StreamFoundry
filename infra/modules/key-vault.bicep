@description('Azure region for the Key Vault.')
param location string

@description('Name of the Key Vault.')
param keyVaultName string

@description('Tenant ID that owns the Key Vault.')
param tenantId string

@description('SKU for the vault.')
@allowed([
  'standard'
  'premium'
])
param skuName string = 'standard'

@description('Access policies to apply to the vault (include Function App managed identity here).')
param accessPolicies array = []

@description('Optional map of secretName -> value to seed into the vault.')
@secure()
param secretValues object = {}

resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: keyVaultName
  location: location
  properties: {
    tenantId: tenantId
    sku: {
      family: 'A'
      name: toUpper(skuName)
    }
    enablePurgeProtection: true
    enableSoftDelete: true
    enableRbacAuthorization: false
    accessPolicies: accessPolicies
    publicNetworkAccess: 'Enabled'
  }
}

var secretNames = [for name in keys(secretValues): name]

resource secrets 'Microsoft.KeyVault/vaults/secrets@2023-07-01' = [for name in secretNames: {
  parent: keyVault
  name: name
  properties: {
    value: secretValues[name]
  }
}]

output vaultName string = keyVault.name
output vaultUri string = keyVault.properties.vaultUri

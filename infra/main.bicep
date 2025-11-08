@description('Azure region for the deployment.')
param location string

@description('Existing storage account name (Event Grid source).')
param storageAccountName string

@description('Existing Function App name (target for Event Grid + managed identity for Key Vault).')
param functionAppName string

@description('Key Vault name that stores application secrets.')
param keyVaultName string

@description('Tenant ID used for Key Vault policies.')
param tenantId string

@description('Optional additional Key Vault access policies.')
param extraAccessPolicies array = []

@description('Secure map of secretName -> value for seeding Key Vault.')
@secure()
param keyVaultSecrets object = {}

@description('Event Grid subscription name.')
param eventSubscriptionName string = 'raw-blob-created'

@description('Raw container name for Event Grid filtering.')
param rawContainerName string = 'raw'

@description('Event types to route from Event Grid.')
param includedEventTypes array = [
  'Microsoft.Storage.BlobCreated'
]

@description('Maximum number of events per batch delivered to the function.')
@minValue(1)
param maxEventsPerBatch int = 1

@description('Preferred batch size (KB).')
@minValue(1)
param preferredBatchSizeInKilobytes int = 64

@description('Maximum delivery attempts before dead-lettering.')
@minValue(1)
param maxDeliveryAttempts int = 30

@description('Time-to-live for undelivered events (minutes).')
@minValue(1)
param eventTimeToLiveInMinutes int = 1440

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' existing = {
  name: storageAccountName
}

resource functionApp 'Microsoft.Web/sites@2023-01-01' existing = {
  name: functionAppName
}

var functionPrincipalId = functionApp.identity.principalId

var functionAccessPolicies = empty(functionPrincipalId) ? [] : [
  {
    tenantId: tenantId
    objectId: functionPrincipalId
    permissions: {
      secrets: [
        'Get'
        'List'
      ]
    }
  }
]

module keyVault 'modules/key-vault.bicep' = {
  name: 'keyVault'
  params: {
    location: location
    keyVaultName: keyVaultName
    tenantId: tenantId
    accessPolicies: concat(functionAccessPolicies, extraAccessPolicies)
    secretValues: keyVaultSecrets
  }
}

module eventGrid 'modules/event-grid-subscription.bicep' = {
  name: 'eventGrid'
  params: {
    storageAccountName: storageAccountName
    functionAppName: functionAppName
    eventSubscriptionName: eventSubscriptionName
    rawContainerName: rawContainerName
    includedEventTypes: includedEventTypes
    maxEventsPerBatch: maxEventsPerBatch
    preferredBatchSizeInKilobytes: preferredBatchSizeInKilobytes
    maxDeliveryAttempts: maxDeliveryAttempts
    eventTimeToLiveInMinutes: eventTimeToLiveInMinutes
  }
}

output keyVaultUri string = keyVault.outputs.vaultUri
output eventGridSubscriptionId string = eventGrid.outputs.subscriptionId

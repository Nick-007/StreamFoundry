@description('Existing storage account to configure.')
param storageAccountName string

@description('Blob containers to ensure exist.')
param blobContainers array = []

@description('Queue names to ensure exist.')
param queueNames array = []

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' existing = {
  name: storageAccountName
}

resource blobServices 'Microsoft.Storage/storageAccounts/blobServices@2023-01-01' existing = {
  parent: storageAccount
  name: 'default'
}

resource queueService 'Microsoft.Storage/storageAccounts/queueServices@2023-01-01' existing = {
  parent: storageAccount
  name: 'default'
}

resource containers 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = [
  for containerName in blobContainers: {
    parent: blobServices
    name: toLower(containerName)
    properties: {
      publicAccess: 'None'
    }
  }
]

resource queues 'Microsoft.Storage/storageAccounts/queueServices/queues@2023-01-01' = [
  for queueName in queueNames: {
    parent: queueService
    name: toLower(queueName)
    properties: {
      metadata: {}
    }
  }
]

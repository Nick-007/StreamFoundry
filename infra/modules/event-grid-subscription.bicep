@description('Existing storage account that emits blob events.')
param storageAccountName string

@description('Existing Function App that hosts the blob_enqueuer function.')
param functionAppName string

@description('Name for the Event Grid subscription.')
param eventSubscriptionName string = 'raw-blob-created'

@description('Blob container prefix to filter on (usually your RAW container).')
param rawContainerName string = 'raw'

@description('Event types that should reach the function.')
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

// Existing resources in the current resource group / subscription
resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' existing = {
  name: storageAccountName
}

resource functionApp 'Microsoft.Web/sites@2023-01-01' existing = {
  name: functionAppName
}

// Ensure the RAW container exists (idempotent if already present)
resource rawContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: storageAccount
  name: 'default/${rawContainerName}'
  properties: {
    publicAccess: 'None'
  }
}

resource blobEnqueuer 'Microsoft.Web/sites/functions@2023-01-01' existing = {
  parent: functionApp
  name: 'blob_enqueuer'
}

resource rawBlobSubscription 'Microsoft.Storage/storageAccounts/providers/eventSubscriptions@2023-06-01-preview' = {
  name: '${storageAccount.name}/Microsoft.EventGrid/${eventSubscriptionName}'
  scope: storageAccount
  properties: {
    eventDeliverySchema: 'EventGridSchema'
    destination: {
      endpointType: 'AzureFunction'
      properties: {
        resourceId: blobEnqueuer.id
        maxEventsPerBatch: maxEventsPerBatch
        preferredBatchSizeInKilobytes: preferredBatchSizeInKilobytes
      }
    }
    filter: {
      isSubjectCaseSensitive: false
      subjectBeginsWith: '/blobServices/default/containers/${rawContainerName}/'
      includedEventTypes: includedEventTypes
    }
    retryPolicy: {
      maxDeliveryAttempts: maxDeliveryAttempts
      eventTimeToLiveInMinutes: eventTimeToLiveInMinutes
    }
  }
  dependsOn: [
    rawContainer
  ]
}

output subscriptionId string = rawBlobSubscription.id

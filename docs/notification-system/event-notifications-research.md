# Event Notification Systems for Storage Platforms - Comprehensive Research

## Overview

This document provides a comprehensive analysis of event notification capabilities across various object storage platforms, evaluating their suitability for event-driven data ingestion in IOMETE Autoloader.

---

## 1. AWS S3 Event Notifications

### Support Status
**Fully Supported** - Industry standard for object storage event notifications.

### Event Types Available

AWS S3 provides comprehensive event types across multiple categories:

**Object Creation Events:**
- `s3:ObjectCreated:Put` - Object created by HTTP PUT
- `s3:ObjectCreated:Post` - Object created by HTTP POST
- `s3:ObjectCreated:Copy` - Object created by S3 copy operation
- `s3:ObjectCreated:CompleteMultipartUpload` - Object created by multipart upload completion
- `s3:ObjectCreated:*` - Wildcard for any object creation

**Object Removal Events:**
- `s3:ObjectRemoved:Delete` - Non-versioned object deleted or versioned object permanently deleted
- `s3:ObjectRemoved:DeleteMarkerCreated` - Delete marker created for versioned object
- `s3:ObjectRemoved:*` - Wildcard for any object deletion

**Object Restore Events (Glacier):**
- `s3:ObjectRestore:Post` - Object restoration initiated
- `s3:ObjectRestore:Completed` - Object restoration completed
- `s3:ObjectRestore:Delete` - Restoration deletion
- `s3:ObjectRestore:*` - Wildcard for any restoration event

**Additional Event Types:**
- Replication events: `s3:Replication:*`
- Lifecycle events: `s3:LifecycleExpiration:*`, `s3:LifecycleTransition`
- Object tagging: `s3:ObjectTagging:*`
- Object ACL: `s3:ObjectAcl:Put`
- Intelligent-Tiering: `s3:IntelligentTiering`
- Reduced Redundancy: `s3:ReducedRedundancyLostObject`

### Notification Delivery Mechanisms

- **Amazon SQS** (Standard queues only, not FIFO)
- **Amazon SNS**
- **AWS Lambda**
- **Amazon EventBridge**

### Configuration Requirements

**SQS Queue Policy Required:**
```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {
      "Service": "s3.amazonaws.com"
    },
    "Action": "SQS:SendMessage",
    "Resource": "arn:aws:sqs:Region:account-id:queue-name",
    "Condition": {
      "ArnLike": {
        "aws:SourceArn": "arn:aws:s3:*:*:bucket-name"
      },
      "StringEquals": {
        "aws:SourceAccount": "bucket-owner-account-id"
      }
    }
  }]
}
```

**Key Constraints:**
- FIFO queues are NOT supported
- Cross-region notifications NOT supported (bucket and SQS must be in same region)
- For encrypted SQS queues, must use customer-managed KMS key with S3 service principal permissions

### Message Format (JSON)

**Event Structure Version:** 2.1, 2.2, or 2.3

**Example Message:**
```json
{
  "Records": [
    {
      "eventVersion": "2.1",
      "eventSource": "aws:s3",
      "awsRegion": "us-west-2",
      "eventTime": "1970-01-01T00:00:00.000Z",
      "eventName": "ObjectCreated:Put",
      "userIdentity": {
        "principalId": "AIDAJDPLRKLG7UEXAMPLE"
      },
      "requestParameters": {
        "sourceIPAddress": "172.16.0.1"
      },
      "responseElements": {
        "x-amz-request-id": "C3D13FE58DE4C810",
        "x-amz-id-2": "FMyUVURIY8/IgAtTv8xRjskZQpcIZ9KG4V5Wp6S7S/JRWeUWerMUE5JgHvANOjpD"
      },
      "s3": {
        "s3SchemaVersion": "1.0",
        "configurationId": "testConfigRule",
        "bucket": {
          "name": "amzn-s3-demo-bucket",
          "ownerIdentity": {
            "principalId": "A3NL1KOZZKExample"
          },
          "arn": "arn:aws:s3:::amzn-s3-demo-bucket"
        },
        "object": {
          "key": "HappyFace.jpg",
          "size": 1024,
          "eTag": "d41d8cd98f00b204e9800998ecf8427e",
          "versionId": "096fKKXTRTtl3on89fVO.nfljtsv6qko",
          "sequencer": "0055AED6DCD90281E5"
        }
      }
    }
  ]
}
```

**Important Notes:**
- Object key names are URL encoded
- Version 2.2 used for cross-region replication events
- Version 2.3 used for Lifecycle, Intelligent-Tiering, ACL, tagging, and restoration delete events

### Practical Assessment for Event-Driven Ingestion

**Excellent Choice:** AWS S3 provides the most mature, reliable, and feature-rich event notification system. Ideal for event-driven ingestion with real-time processing capabilities.

---

## 2. MinIO Event Notifications

### Support Status
**Fully Supported** - S3-compatible with extended capabilities.

### Event Types Available

MinIO supports all standard S3 event types and is fully compatible with the S3 event notification format.

### Notification Delivery Mechanisms

MinIO provides extensive notification target support:

- **AMQP** (RabbitMQ)
- **MQTT** (supports MQTT 3.1 and 3.1.1 via tcp://, tls://, or ws://)
- **NATS**
- **NSQ**
- **Elasticsearch**
- **Kafka**
- **MySQL**
- **PostgreSQL**
- **Redis**
- **Webhook** (HTTP/HTTPS endpoints)

### ARN Format

MinIO uses SQS-compatible ARN format for internal identification:
- `arn:minio:sqs::1:kafka`
- `arn:minio:sqs::1:nats`
- `arn:minio:sqs::1:mqtt`
- `arn:minio:sqs::1:webhook`

**Note:** These are MinIO internal identifiers, not AWS SQS integration.

### S3 Compatibility

**Yes** - MinIO bucket notifications are fully compatible with Amazon S3 Event Notifications format.

### Message Format Example

```json
{
  "EventName": "s3:ObjectCreated:Put",
  "Key": "Downloads/to-kuro.sh",
  "Records": [
    {
      "eventVersion": "2.0",
      "eventSource": "minio:s3",
      "awsRegion": "",
      "eventTime": "2020-12-08T13:40:56.970Z",
      "eventName": "s3:ObjectCreated:Put",
      "userIdentity": {
        "principalId": "minio"
      },
      "requestParameters": {
        "accessKey": "minio",
        "region": "",
        "sourceIPAddress": "192.168.1.134"
      },
      "responseElements": {
        "content-length": "0",
        "x-amz-request-id": "164EC17C5E9BEB3E",
        "x-minio-deployment-id": "d3d81f71-a06c-451e-89be-b1dc4e891054",
        "x-minio-origin-endpoint": "https://192.168.1.36:9000"
      },
      "s3": {
        "s3SchemaVersion": "1.0",
        "configurationId": "Config",
        "bucket": {
          "name": "Downloads",
          "ownerIdentity": {
            "principalId": "minio"
          },
          "arn": "arn:aws:s3:::Downloads"
        },
        "object": {
          "key": "testscript.sh",
          "size": 337,
          "eTag": "5f604e1b35b1ca405b35503b86b56d51",
          "contentType": "application/x-sh",
          "userMetadata": {
            "content-type": "application/x-sh"
          },
          "sequencer": "164EC17C6153CB1E"
        }
      }
    }
  ]
}
```

**Notable Differences from AWS:**
- `eventSource` is `minio:s3` instead of `aws:s3`
- Includes MinIO-specific response elements like `x-minio-deployment-id` and `x-minio-origin-endpoint`
- Region field may be empty in MinIO deployments
- Otherwise maintains full S3 compatibility

### Practical Assessment for Event-Driven Ingestion

**Excellent Choice:** MinIO provides S3-compatible event notifications with more flexible delivery targets. Particularly valuable for on-premises deployments or when you need to integrate with non-AWS message brokers like Kafka, NATS, or MQTT.

---

## 3. Azure Blob Storage Event Notifications

### Support Status
**Fully Supported** - via Azure Event Grid integration.

### Storage Account Requirements

Only the following storage account types support Event Grid:
- **StorageV2** (general purpose v2)
- **BlockBlobStorage**
- **BlobStorage**

### Event Types Available

**Blob Storage Events:**
- `Microsoft.Storage.BlobCreated` - Blob created or replaced
- `Microsoft.Storage.BlobDeleted` - Blob deleted
- `Microsoft.Storage.BlobTierChanged` - Blob access tier changed
- `Microsoft.Storage.AsyncOperationInitiated` - Async operations (e.g., archive tier restoration)

**Data Lake Storage Gen2 Events:**
- `Microsoft.Storage.BlobCreated`
- `Microsoft.Storage.BlobDeleted`
- `Microsoft.Storage.BlobRenamed`
- `Microsoft.Storage.DirectoryCreated`
- `Microsoft.Storage.DirectoryRenamed`
- `Microsoft.Storage.DirectoryDeleted`

**SFTP Events:**
- Same directory and blob operations via SFTP protocol

**Policy Events:**
- `Microsoft.Storage.BlobInventoryPolicyCompleted`
- `Microsoft.Storage.LifecyclePolicyCompleted`

### Notification Delivery Mechanisms

Azure Event Grid can deliver to:
- **Azure Functions**
- **Azure Logic Apps**
- **Event Hubs**
- **Service Bus**
- **Storage Queues**
- **Webhooks** (custom HTTP endpoints)
- **Hybrid Connections**

### Message Format (Cloud Event Schema)

**Subject Format:** `/blobServices/default/containers/<containername>/blobs/<blobname>`

**Example Event:**
```json
[{
  "source": "/subscriptions/{subscription-id}/resourceGroups/Storage/providers/Microsoft.Storage/storageAccounts/my-storage-account",
  "subject": "/blobServices/default/containers/test-container/blobs/new-file.txt",
  "type": "Microsoft.Storage.BlobCreated",
  "time": "2017-06-26T18:41:00.9584103Z",
  "id": "831e1650-001e-001b-66ab-eeb76e069631",
  "data": {
    "api": "PutBlockList",
    "clientRequestId": "6d79dbfb-0e37-4fc4-981f-442c9ca65760",
    "requestId": "831e1650-001e-001b-66ab-eeb76e000000",
    "eTag": "0x8D4BCC2E4835CD0",
    "contentType": "text/plain",
    "contentLength": 524288,
    "blobType": "BlockBlob",
    "accessTier": "Default",
    "url": "https://my-storage-account.blob.core.windows.net/testcontainer/new-file.txt",
    "sequencer": "00000000000004420000000000028963",
    "storageDiagnostics": {
      "batchId": "b68529f3-68cd-4744-baa4-3c0498ec19f0"
    }
  },
  "specversion": "1.0"
}]
```

**Key Data Properties:**
- API operation name
- Request IDs (client and service)
- ETag
- Content metadata (type, length)
- Blob type and access tier
- Full URL path
- Sequencer for event ordering
- Storage diagnostics

### S3 Compatibility

**No** - Azure uses a completely different event schema (CloudEvents standard) and is not compatible with S3 event notification format.

### Practical Assessment for Event-Driven Ingestion

**Good Choice for Azure Deployments:** Robust event system with Azure-native integration. However, event schema is Azure-specific and requires different parsing logic than S3. Event Grid provides reliable delivery with retry policies and dead-lettering.

---

## 4. Google Cloud Storage Event Notifications

### Support Status
**Fully Supported** - via Cloud Pub/Sub integration.

### Event Types Available

Four standard event types:

- `OBJECT_FINALIZE` - New version of object created (upload, copy, rewrite, or compose)
- `OBJECT_METADATA_UPDATE` - Object metadata updated
- `OBJECT_DELETE` - Object deleted
- `OBJECT_ARCHIVE` - Object archived (versioning enabled)

**Default Behavior:** If event types not specified, notifications sent for ALL event types.

**Special Behavior for Object Replacement:**
When replacing an object with the same name:
- `OBJECT_FINALIZE` event for new version
- `OBJECT_ARCHIVE` or `OBJECT_DELETE` event for replaced object
- Events include generation numbers for tracking

### Notification Delivery Mechanism

**Cloud Pub/Sub** - All notifications delivered via Google Cloud Pub/Sub topics.

### Message Structure

**Two Components:**
1. **Attributes** - Key-value pairs describing the event
2. **Payload** - String containing object metadata

**Standard Attributes (Always Included):**
- `notificationConfig` - Identifier for triggering notification configuration
- `eventType` - Type of event (e.g., OBJECT_FINALIZE)
- `payloadFormat` - Format of object payload
- `bucketId` - Name of bucket containing changed object
- `objectId` - Name of changed object
- `objectGeneration` - Generation number of changed object
- `eventTime` - Time event occurred (RFC 3339 format)

**Optional Attributes (Conditional):**
- `overwrittenByGeneration` - Generation number of replacement object (OBJECT_ARCHIVE/OBJECT_DELETE only)
- `overwroteGeneration` - Generation number of replaced object (OBJECT_FINALIZE only)

**Custom Attributes:**
Up to 10 custom key-value pairs can be defined per notification configuration.

**Payload Format Options:**
- `NONE` - No payload included
- `JSON_API_V1` - UTF-8 string with object's metadata resource representation

### S3 Compatibility

**No** - Google Cloud Storage uses its own Pub/Sub message format with attributes and payload structure. Not compatible with S3 event notification format.

### Configuration

Configure via:
- `gcloud storage` CLI commands
- Google Cloud Storage JSON API (POST notificationConfigs)
- Client libraries

### Limitations

For other GCS events (bucket operations, object reads), use Cloud Audit Logs routed to Pub/Sub.

### Practical Assessment for Event-Driven Ingestion

**Good Choice for GCP Deployments:** Reliable Pub/Sub-based system with strong ordering guarantees. However, requires GCP-specific integration code. Event format is simpler than S3 but requires different parsing logic.

---

## 5. Dell EMC ECS (Elastic Cloud Storage)

### Support Status
**Supported** - S3-compatible event notifications available.

### Event Types Available

Dell EMC ObjectScale (newer platform building on ECS) supports standard S3 event types:
- `s3:ObjectCreated:Put`
- `s3:ObjectCreated:*`
- `s3:ObjectRemoved:*`
- Other standard S3 event types

### Notification Delivery Mechanisms

**For Bucket Events (S3-compatible):**
- Conforms to S3 event notification structure standard
- Supports standard S3 notification API operations

**For System Events:**
- **SNMP** - Network-managed device status and statistics
- **Syslog** - Centralized storage and retrieval of system log messages

### API Operations

Dell ECS supports standard S3 bucket notification operations:

- **PUT Bucket Notification** - Create or replace notification configuration
- **GET Bucket Notification Configuration** - Retrieve current configuration

**API Endpoint Format:**
- `PUT /?notification` - Configure bucket notifications
- `GET /?notification` - Retrieve bucket notification configuration

### Configuration Requirements

- Only bucket owner or users with proper permissions can create/replace notification configuration
- BaseUrl must be pre-configured using ECS Management API or ECS Portal
- Configuration uses XML format defining event types, destinations, and filter rules

### S3 Compatibility

**Yes** - Dell EMC ObjectScale's event notification structure conforms to the S3 event notification structure standard.

### Practical Assessment for Event-Driven Ingestion

**Good Choice for Enterprise Deployments:** Dell ECS provides S3-compatible event notifications suitable for on-premises enterprise storage. However, documentation is less accessible than cloud providers. Best suited for organizations already using Dell storage infrastructure.

---

## 6. Pure Storage FlashBlade

### Support Status
**Unknown/Undocumented** - S3 event notifications support could not be confirmed.

### S3 API Support

Pure Storage FlashBlade provides:
- **S3-compatible object storage API**
- Ground-up native implementation (not a gateway/shim)
- Tested with AWS S3 SDKs (Java, Python, GO, C#, .NET)
- S3 versioning support
- Pre-signed URLs (PUT/GET operations)
- Multipart upload support
- S3 Fast Copy for replication

### Recent Developments (2024-2025)

- **S3 over RDMA** - Optimized for AI/ML workflows
- Preliminary results: ~250GB/s throughput for 5-chassis system
- Focus on performance for demanding AI workloads

### Event Notification Support

**Status: UNCONFIRMED**

Search results did not reveal specific documentation about:
- S3 event notifications to SNS/SQS/webhooks
- Bucket notification configuration API
- Event types or message format

### Documentation Access

FlashBlade S3 API documentation is available on Pure Storage support portal:
- `support.purestorage.com/FlashBlade/Purity_FB/PurityFB_REST_API/S3_Object_Store_REST_API/`
- PDF documentation: "FlashBlade Object Store S3 REST API Version 2.0"
- **Note:** May require customer/partner credentials

### Recommendations for Verification

To determine FlashBlade's event notification capabilities:
1. Check official FlashBlade S3 API documentation on Pure Storage support portal
2. Contact Pure Storage technical support directly
3. Review FlashBlade release notes for specific Purity//FB version

### Practical Assessment for Event-Driven Ingestion

**Cannot Recommend Without Confirmation:** While FlashBlade provides solid S3 API compatibility for basic operations, event notification support is undocumented in public sources. For event-driven ingestion, would need to:
- Confirm support with Pure Storage directly
- Or fall back to scheduled batch ingestion approach

---

## Summary Comparison Table

| Platform | Event Notifications | S3 Compatible | Delivery Mechanisms | Best Use Case |
|----------|---------------------|---------------|---------------------|---------------|
| **AWS S3** | Full Support | N/A (Standard) | SQS, SNS, Lambda, EventBridge | Cloud-native AWS deployments, real-time ingestion |
| **MinIO** | Full Support | Yes | AMQP, MQTT, NATS, Kafka, Webhook, MySQL, PostgreSQL, Redis, Elasticsearch | On-premises, multi-protocol integration, flexible message brokers |
| **Azure Blob** | Full Support | No (CloudEvents) | Event Grid → Functions, Logic Apps, Webhooks, Event Hubs, Service Bus | Azure-native deployments |
| **Google Cloud Storage** | Full Support | No (Pub/Sub) | Cloud Pub/Sub | GCP-native deployments |
| **Dell EMC ECS** | Supported | Yes | S3-compatible notifications, SNMP, Syslog | Enterprise on-premises storage |
| **Pure Storage FlashBlade** | Unknown | Partial (S3 API) | Unknown | High-performance workloads (verify support needed) |

---

## Practical Recommendations for IOMETE Autoloader

### Event-Driven Ingestion: Best Candidates

**Tier 1 - Fully Supported, Production Ready:**
1. **AWS S3** - Industry standard, most mature
2. **MinIO** - Excellent S3 compatibility, flexible delivery targets
3. **Dell EMC ECS** - S3-compatible, enterprise-grade

**Tier 2 - Supported but Requires Platform-Specific Code:**
4. **Azure Blob Storage** - Different event schema, requires separate implementation
5. **Google Cloud Storage** - Different message format, requires separate implementation

**Tier 3 - Verification Needed:**
6. **Pure Storage FlashBlade** - Cannot confirm event notification support

### Scheduled Batch Ingestion: Universal Fallback

For platforms without event notification support (or as a user preference), scheduled batch ingestion remains the reliable universal approach:

**Advantages:**
- Works with ALL storage platforms
- More cost-effective for infrequent data arrival
- Simpler configuration
- Lower operational complexity
- No message broker infrastructure required

**When to Recommend Batch Over Events:**
- Data arrives on predictable schedule (daily, hourly, etc.)
- Small file volumes
- Cost optimization priority
- Platform event notification support unclear/unavailable
- Simpler debugging and testing

### Implementation Strategy

**Phase 1: S3-Compatible Event Notifications**
- Implement for AWS S3, MinIO, and Dell ECS
- Use standard S3 event notification format
- Single codebase serves multiple platforms

**Phase 2: Cloud-Specific Implementations**
- Azure Event Grid integration
- Google Cloud Pub/Sub integration
- Separate message parsers per platform

**Phase 3: Verification and Testing**
- Pure Storage FlashBlade support investigation
- Other S3-compatible storage providers as needed

**Always Available: Scheduled Batch Mode**
- Maintain as primary option for all platforms
- Position event-driven as optional performance optimization
- Provides guaranteed compatibility across all storage types

---

## Conclusion

Event notification systems vary significantly across storage platforms:

- **AWS S3 and MinIO** offer the most straightforward path for event-driven ingestion with S3-compatible formats
- **Azure and Google Cloud** provide robust event systems but require platform-specific implementations
- **Dell ECS** offers S3 compatibility for enterprise deployments
- **Pure Storage FlashBlade** requires verification before committing to event-driven architecture

For IOMETE Autoloader, a hybrid approach is recommended:
1. Implement event-driven ingestion for S3-compatible platforms first
2. Maintain scheduled batch ingestion as a universal fallback
3. Add cloud-specific event handlers in later phases
4. Let users choose based on their requirements and platform capabilities

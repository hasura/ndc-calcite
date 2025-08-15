# Storage Providers Guide

Comprehensive guide to all storage providers supported by the Apericio JDBC Driver for accessing data from various sources and cloud platforms.

## Overview

The Apericio driver supports multiple storage providers to access data from:
- **Local file systems** - Traditional file and directory access
- **Cloud storage** - S3, Azure Blob, Google Cloud Storage
- **Enterprise systems** - SharePoint, Microsoft Graph (OneDrive/Teams)
- **Web APIs** - HTTP/HTTPS with advanced authentication
- **Remote file systems** - SFTP, FTP

Each provider has extensive configuration options for authentication, performance tuning, and security.

## Local File System

### Basic Local Access
```java
// Simple directory access
String url = "jdbc:apericio:/data/warehouse";
Connection conn = DriverManager.getConnection(url);
```

### Advanced Local Configuration
```json
{
  "storageType": "local",
  "storageConfig": {
    "basePath": "/data/warehouse",
    "recursive": true,
    "followSymlinks": false,
    "readOnly": true,
    "filePermissions": "644",
    "directoryPermissions": "755"
  }
}
```

## Amazon S3

### Basic S3 Access
```java
// Using AWS credentials chain (recommended)
String url = "jdbc:apericio:model=/etc/s3-config.json";
Connection conn = DriverManager.getConnection(url);
```

### AWS Credentials Chain Configuration
```json
{
  "name": "s3_data_lake",
  "operand": {
    "storageType": "s3",
    "storageConfig": {
      "bucket": "company-data-lake",
      "region": "us-east-1",
      "prefix": "warehouse/",
      
      // Uses AWS credentials chain automatically:
      // 1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
      // 2. Java system properties (aws.accessKeyId, aws.secretKey)
      // 3. Credential profiles file (~/.aws/credentials)
      // 4. IAM instance profile credentials
      // 5. IAM assume role credentials
    }
  }
}
```

### Explicit S3 Credentials (Not Recommended for Production)
```json
{
  "storageType": "s3",
  "storageConfig": {
    "bucket": "data-bucket",
    "region": "us-west-2",
    "accessKeyId": "${AWS_ACCESS_KEY_ID}",
    "secretAccessKey": "${AWS_SECRET_ACCESS_KEY}",
    "sessionToken": "${AWS_SESSION_TOKEN}"
  }
}
```

### S3 with Cross-Account Role Assumption
```json
{
  "storageType": "s3",
  "storageConfig": {
    "bucket": "partner-data-bucket",
    "region": "us-east-1",
    "roleArn": "arn:aws:iam::123456789012:role/DataAccessRole",
    "externalId": "unique-external-id",
    "sessionName": "apericio-data-access"
  }
}
```

### S3 Performance Optimization
```json
{
  "storageType": "s3",
  "storageConfig": {
    "bucket": "large-data-bucket",
    "region": "us-east-1",
    "multipartThreshold": "64MB",
    "multipartSize": "16MB",
    "maxConnections": 50,
    "connectionTimeout": 60000,
    "socketTimeout": 60000,
    "requestTimeout": 300000,
    "clientExecutionTimeout": 600000
  }
}
```

### S3 with Server-Side Encryption
```json
{
  "storageType": "s3",
  "storageConfig": {
    "bucket": "secure-data-bucket",
    "region": "us-east-1",
    "serverSideEncryption": "AES256",
    "kmsKeyId": "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
    "bucketKeyEnabled": true
  }
}
```

## HTTP/HTTPS APIs

### Phase 1: Simple Authentication

#### Bearer Token Authentication
```json
{
  "storageType": "http",
  "storageConfig": {
    "baseUrl": "https://api.example.com/data",
    "method": "GET",
    "authConfig": {
      "bearerToken": "${API_TOKEN}"
    },
    "headers": {
      "Accept": "application/json",
      "User-Agent": "Apericio-JDBC/1.0"
    }
  }
}
```

#### API Key Authentication
```json
{
  "storageType": "http",
  "storageConfig": {
    "baseUrl": "https://api.example.com/data",
    "authConfig": {
      "apiKey": "${API_KEY}",
      "apiKeyHeader": "X-API-Key"
    }
  }
}
```

#### Basic Authentication
```json
{
  "storageType": "http",
  "storageConfig": {
    "baseUrl": "https://api.example.com/data",
    "authConfig": {
      "username": "${API_USER}",
      "password": "${API_PASS}"
    }
  }
}
```

### Phase 2: External Token Management

#### Command-Based Token Retrieval
```json
{
  "storageType": "http",
  "storageConfig": {
    "baseUrl": "https://api.example.com/data",
    "authConfig": {
      "tokenCommand": "aws sts get-session-token --output text --query Credentials.SessionToken",
      "tokenPattern": "(.+)",
      "cacheTtl": 3600
    }
  }
}
```

#### File-Based Token Management
```json
{
  "storageType": "http",
  "storageConfig": {
    "baseUrl": "https://api.example.com/data", 
    "authConfig": {
      "tokenFile": "/var/secrets/api-token.txt",
      "tokenPattern": "token=(.+)",
      "fileWatchEnabled": true,
      "cacheTtl": 1800
    }
  }
}
```

#### Environment Variable Token
```json
{
  "storageType": "http",
  "storageConfig": {
    "baseUrl": "https://api.example.com/data",
    "authConfig": {
      "tokenEnv": "DYNAMIC_API_TOKEN",
      "refreshInterval": "15 minutes"
    }
  }
}
```

#### OAuth2 Token Endpoint
```json
{
  "storageType": "http",
  "storageConfig": {
    "baseUrl": "https://api.example.com/data",
    "authConfig": {
      "tokenEndpoint": "https://auth.example.com/oauth/token",
      "clientId": "${OAUTH_CLIENT_ID}",
      "clientSecret": "${OAUTH_CLIENT_SECRET}",
      "scope": "data:read",
      "grantType": "client_credentials",
      "cacheTtl": 3600
    }
  }
}
```

### Phase 2.5: Template-Based Auth Headers
```json
{
  "storageType": "http",
  "storageConfig": {
    "baseUrl": "https://api.example.com/data",
    "authConfig": {
      "authHeaders": {
        "Authorization": "Bearer ${OAUTH_TOKEN}",
        "X-API-Key": "${API_KEY}",
        "X-User-ID": "${USER_ID}",
        "X-Tenant": "${TENANT_ID}"
      }
    }
  }
}
```

### Phase 3: Full Proxy Delegation
```json
{
  "storageType": "http",
  "storageConfig": {
    "baseUrl": "https://api.example.com/data",
    "authConfig": {
      "proxyEndpoint": "https://auth-proxy.company.com/authenticate",
      "proxyHeaders": {
        "X-Original-URL": "https://api.example.com/data",
        "X-Service": "apericio-jdbc"
      }
    }
  }
}
```

### HTTP POST with Request Body
```json
{
  "storageType": "http",
  "storageConfig": {
    "baseUrl": "https://api.example.com/query",
    "method": "POST",
    "body": "{\"query\": \"SELECT * FROM data WHERE date >= '2024-01-01'\"}",
    "mimeType": "application/json",
    "authConfig": {
      "bearerToken": "${API_TOKEN}"
    },
    "headers": {
      "Content-Type": "application/json",
      "Accept": "application/json"
    }
  }
}
```

## SharePoint Online

### Modern Authentication (Recommended)
```json
{
  "storageType": "sharepoint",
  "storageConfig": {
    "siteUrl": "https://company.sharepoint.com/sites/data",
    "clientId": "${SHAREPOINT_CLIENT_ID}",
    "clientSecret": "${SHAREPOINT_CLIENT_SECRET}",
    "tenantId": "${TENANT_ID}",
    "libraryName": "Documents",
    "path": "/reports/2024",
    "authType": "modern"
  }
}
```

### Certificate-Based Authentication
```json
{
  "storageType": "sharepoint",
  "storageConfig": {
    "siteUrl": "https://company.sharepoint.com/sites/data",
    "clientId": "${SHAREPOINT_CLIENT_ID}",
    "certificatePath": "/etc/ssl/certs/sharepoint.p12",
    "certificatePassword": "${CERT_PASSWORD}",
    "tenantId": "${TENANT_ID}",
    "authType": "certificate"
  }
}
```

### Legacy Authentication (For On-Premises)
```json
{
  "storageType": "sharepoint",
  "storageConfig": {
    "siteUrl": "https://sharepoint.company.com/sites/data",
    "username": "${SHAREPOINT_USER}",
    "password": "${SHAREPOINT_PASS}",
    "domain": "COMPANY",
    "authType": "legacy",
    "ntlmEnabled": true
  }
}
```

### SharePoint with File Type Filtering
```json
{
  "storageType": "sharepoint",
  "storageConfig": {
    "siteUrl": "https://company.sharepoint.com/sites/data",
    "clientId": "${SHAREPOINT_CLIENT_ID}",
    "clientSecret": "${SHAREPOINT_CLIENT_SECRET}",
    "tenantId": "${TENANT_ID}",
    "libraryName": "Documents",
    "path": "/reports",
    "fileExtensions": ["xlsx", "csv", "docx"],
    "excludePatterns": ["temp", "draft"],
    "maxFileSize": "100MB"
  }
}
```

## Microsoft Graph (OneDrive/Teams)

### OneDrive Business Access
```json
{
  "storageType": "graph",
  "storageConfig": {
    "tenantId": "${TENANT_ID}",
    "clientId": "${CLIENT_ID}",
    "clientSecret": "${CLIENT_SECRET}",
    "scope": "Files.Read.All",
    "driveType": "onedrive",
    "userId": "user@company.com",
    "path": "/Analytics/Data"
  }
}
```

### Teams Files Access
```json
{
  "storageType": "graph",
  "storageConfig": {
    "tenantId": "${TENANT_ID}",
    "clientId": "${CLIENT_ID}",
    "clientSecret": "${CLIENT_SECRET}",
    "scope": "Files.Read.All Sites.Read.All",
    "driveType": "teams",
    "teamId": "${TEAM_ID}",
    "channelId": "${CHANNEL_ID}",
    "path": "/Data"
  }
}
```

### SharePoint Site via Graph API
```json
{
  "storageType": "graph",
  "storageConfig": {
    "tenantId": "${TENANT_ID}",
    "clientId": "${CLIENT_ID}",
    "clientSecret": "${CLIENT_SECRET}",
    "scope": "Sites.Read.All",
    "driveType": "sharepoint",
    "siteId": "${SITE_ID}",
    "driveId": "${DRIVE_ID}",
    "path": "/Shared Documents/Reports"
  }
}
```

## SFTP/FTP

### SFTP with Password Authentication
```json
{
  "storageType": "sftp",
  "storageConfig": {
    "host": "sftp.example.com",
    "port": 22,
    "username": "${SFTP_USER}",
    "password": "${SFTP_PASS}",
    "path": "/data/exports",
    "timeout": 30000,
    "keepAlive": true
  }
}
```

### SFTP with Private Key Authentication
```json
{
  "storageType": "sftp",
  "storageConfig": {
    "host": "sftp.example.com",
    "port": 22,
    "username": "${SFTP_USER}",
    "privateKeyPath": "${HOME}/.ssh/id_rsa",
    "passphrase": "${KEY_PASSPHRASE}",
    "path": "/data/exports",
    "knownHostsPath": "${HOME}/.ssh/known_hosts",
    "strictHostKeyChecking": true
  }
}
```

### SFTP with Jump Host (Bastion)
```json
{
  "storageType": "sftp",
  "storageConfig": {
    "host": "internal-sftp.company.com",
    "port": 22,
    "username": "${SFTP_USER}",
    "privateKeyPath": "${HOME}/.ssh/id_rsa",
    "path": "/data",
    "jumpHost": {
      "host": "bastion.company.com",
      "port": 22,
      "username": "${BASTION_USER}",
      "privateKeyPath": "${HOME}/.ssh/bastion_key"
    }
  }
}
```

### FTP (Plain Text - Not Recommended)
```json
{
  "storageType": "ftp",
  "storageConfig": {
    "host": "ftp.example.com",
    "port": 21,
    "username": "${FTP_USER}",
    "password": "${FTP_PASS}",
    "path": "/data",
    "passive": true,
    "binary": true
  }
}
```

### FTPS (FTP over SSL/TLS)
```json
{
  "storageType": "ftps",
  "storageConfig": {
    "host": "ftps.example.com", 
    "port": 990,
    "username": "${FTPS_USER}",
    "password": "${FTPS_PASS}",
    "path": "/data",
    "implicit": true,
    "trustAllCertificates": false,
    "certificatePath": "/etc/ssl/certs/ftps-server.crt"
  }
}
```

## Multi-Storage Schema Examples

### Hybrid Cloud Architecture
```json
{
  "version": "1.0",
  "defaultSchema": "unified_data",
  "schemas": [
    {
      "name": "s3_data_lake",
      "operand": {
        "storageType": "s3",
        "storageConfig": {
          "bucket": "company-data-lake",
          "region": "us-east-1",
          "prefix": "processed/"
        },
        "executionEngine": "parquet"
      }
    },
    {
      "name": "sharepoint_reports",
      "operand": {
        "storageType": "sharepoint",
        "storageConfig": {
          "siteUrl": "https://company.sharepoint.com/sites/reports",
          "clientId": "${SHAREPOINT_CLIENT_ID}",
          "clientSecret": "${SHAREPOINT_CLIENT_SECRET}",
          "tenantId": "${TENANT_ID}"
        },
        "executionEngine": "linq4j"
      }
    },
    {
      "name": "partner_sftp",
      "operand": {
        "storageType": "sftp",
        "storageConfig": {
          "host": "data.partner.com",
          "username": "${PARTNER_USER}",
          "privateKeyPath": "${HOME}/.ssh/partner_key",
          "path": "/exports"
        },
        "executionEngine": "arrow"
      }
    },
    {
      "name": "api_data",
      "operand": {
        "storageType": "http",
        "storageConfig": {
          "baseUrl": "https://api.vendor.com/data",
          "authConfig": {
            "bearerToken": "${VENDOR_API_TOKEN}"
          }
        },
        "refreshInterval": "1 hour"
      }
    }
  ],
  "materializedViews": [{
    "view": "unified_sales",
    "table": "sales_cache",
    "sql": "SELECT 'internal' as source, * FROM s3_data_lake.sales UNION ALL SELECT 'partner' as source, * FROM partner_sftp.sales_feed UNION ALL SELECT 'api' as source, * FROM api_data.live_sales"
  }]
}
```

### Cross-Environment Data Integration
```sql
-- Query across multiple storage providers
SELECT 
  internal.region,
  internal.sales_internal,
  partner.sales_partner,
  api.live_metrics,
  sharepoint.forecast
FROM s3_data_lake.regional_sales internal
LEFT JOIN partner_sftp.partner_sales partner 
  ON internal.region = partner.region
LEFT JOIN api_data.live_dashboard api 
  ON internal.region = api.region  
LEFT JOIN sharepoint_reports.forecasts sharepoint 
  ON internal.region = sharepoint.region
WHERE internal.quarter = '2024-Q1';
```

## Performance Optimization by Provider

### S3 Optimization
```json
{
  "storageType": "s3",
  "storageConfig": {
    "bucket": "high-throughput-bucket",
    "region": "us-east-1",
    "maxConnections": 100,
    "multipartThreshold": "32MB",
    "transferAcceleration": true,
    "requestPayer": "Requester",
    "pathStyleAccess": false
  }
}
```

### HTTP API Optimization
```json
{
  "storageType": "http",
  "storageConfig": {
    "baseUrl": "https://fast-api.example.com/data",
    "connectionPoolSize": 20,
    "connectionTimeout": 5000,
    "readTimeout": 30000,
    "retryPolicy": {
      "maxRetries": 3,
      "backoffStrategy": "exponential",
      "retryDelay": 1000
    },
    "compression": "gzip"
  }
}
```

### SFTP Optimization
```json
{
  "storageType": "sftp",
  "storageConfig": {
    "host": "high-speed-sftp.example.com",
    "maxChannels": 10,
    "bufferSize": "64KB",
    "compression": "zlib",
    "keepAlive": true,
    "keepAliveInterval": 30,
    "prefetchSize": "32KB"
  }
}
```

## Security Best Practices

### Environment Variable Management
```bash
# Recommended: Use .env files or secure secret management
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export SHAREPOINT_CLIENT_SECRET="your-sharepoint-secret"
export API_TOKEN="your-api-token"
```

### Credential Rotation Example
```json
{
  "storageType": "http",
  "storageConfig": {
    "baseUrl": "https://api.example.com/data",
    "authConfig": {
      "tokenCommand": "/usr/local/bin/get-rotating-token.sh",
      "cacheTtl": 1800,
      "refreshBeforeExpiry": 300
    }
  }
}
```

### Certificate-Based Security
```json
{
  "storageType": "https",
  "storageConfig": {
    "baseUrl": "https://secure-api.example.com/data",
    "sslConfig": {
      "clientCertPath": "/etc/ssl/client.p12",
      "clientCertPassword": "${CLIENT_CERT_PASS}",
      "trustStorePath": "/etc/ssl/truststore.jks",
      "trustStorePassword": "${TRUST_STORE_PASS}",
      "verifyHostname": true
    }
  }
}
```

This comprehensive storage provider configuration enables the Apericio driver to connect to virtually any data source with enterprise-grade security and performance optimization.
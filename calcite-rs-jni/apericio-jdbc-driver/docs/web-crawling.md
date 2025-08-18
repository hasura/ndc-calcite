# HTML Web Crawling Guide

Comprehensive guide to using the Apericio JDBC Driver's advanced HTML web crawling capabilities for automated data discovery and extraction.

## Overview

The HTML crawler is one of the most powerful features of the Apericio driver, capable of:
- **Discovering data files** linked from web pages
- **Extracting HTML tables** directly from pages
- **Following links** with configurable depth and filtering
- **Caching content** for performance and reliability
- **Handling authentication** for protected sites

## Quick Start

### Basic Web Crawling Setup
```java
// Simple crawling with model file
String url = "jdbc:apericio:model=/path/to/crawler-config.json";
Connection conn = DriverManager.getConnection(url);

// Query discovered tables
ResultSet rs = conn.createStatement().executeQuery(
    "SELECT * FROM web_data.financial_report__table_1"
);
```

**Basic crawler-config.json:**
```json
{
  "version": "1.0",
  "defaultSchema": "web_data",
  "schemas": [{
    "name": "web_data",
    "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
    "operand": {
      "storageType": "http",
      "baseUrl": "https://reports.company.com",
      "crawlerConfig": {
        "enabled": true,
        "maxDepth": 2,
        "linkPattern": ".*\\/(data|reports)\\/.*"
      }
    }
  }]
}
```

## Core Crawler Configuration

### Navigation Control
```json
{
  "crawlerConfig": {
    "enabled": true,
    "maxDepth": 3,                    // How deep to crawl from starting page
    "maxPages": 100,                  // Maximum total pages to process
    "followExternalLinks": false,     // Stay within same domain
    "requestDelay": "2 seconds"       // Polite crawling delay
  }
}
```

**Depth Examples:**
- `maxDepth: 0` - Only process the starting page
- `maxDepth: 1` - Process starting page + directly linked pages
- `maxDepth: 2` - Process starting page + 2 levels of linked pages
- `maxDepth: 3` - Three levels deep (can discover hundreds of pages)

### Link Filtering
```json
{
  "crawlerConfig": {
    "linkPattern": ".*\\/(data|reports|analytics)\\/.*",
    "excludePatterns": [".*\\/temp\\/.*", ".*\\/archive\\/.*"],
    "allowedDomains": ["data.company.com", "reports.company.com"]
  }
}
```

**Pattern Examples:**
```json
{
  "linkPattern": ".*\\/2024\\/.*",           // Only 2024 content
  "linkPattern": ".*\\.(html|htm)$",         // Only HTML pages
  "linkPattern": ".*(quarterly|annual).*",   // Only quarterly/annual reports
  "excludePatterns": [".*\\/draft\\/.*"]     // Skip draft content
}
```

## Data File Discovery

### File Pattern Matching
```json
{
  "crawlerConfig": {
    "dataFilePattern": ".*\\.(csv|xlsx|json|parquet)$",
    "dataFileExcludePattern": ".*\\/(temp|backup)\\/.*",
    "allowedFileExtensions": [
      "csv", "xlsx", "json", "tsv", 
      "parquet", "docx", "pptx", "pdf"
    ]
  }
}
```

### File Size Management
```json
{
  "crawlerConfig": {
    "maxDataFileSize": "100MB",
    "extensionSizeLimits": {
      "csv": "50MB",
      "xlsx": "100MB",
      "json": "20MB", 
      "parquet": "200MB",
      "pdf": "25MB",
      "docx": "50MB",
      "pptx": "100MB"
    },
    "enforceContentLengthHeader": true
  }
}
```

## HTML Table Extraction

### Table Discovery Settings
```json
{
  "crawlerConfig": {
    "generateTablesFromHtml": true,
    "htmlTableMinRows": 3,            // Skip header-only tables
    "htmlTableMaxRows": 50000         // Prevent memory issues
  }
}
```

### Table Naming Convention
When HTML tables are extracted, they're named using this pattern:
```
{page-name}__{table-index}
{page-name}__{table-title}    // If table has title/caption
```

**Examples:**
```
financial-report.html → financial_report__table_1, financial_report__table_2
quarterly-data.html with titled tables → quarterly_data__revenue, quarterly_data__expenses
```

## Refresh Strategies and Caching

### Understanding Refresh Behavior

The crawler supports multiple refresh strategies to balance data freshness with performance:

#### 1. **Time-Based Refresh** (Most Common)
```json
{
  "crawlerConfig": {
    "refreshInterval": "2 hours",         // Re-crawl every 2 hours
    "dataFileCacheTTL": "4 hours",       // Cache files for 4 hours
    "htmlCacheTTL": "1 hour"             // Cache HTML for 1 hour
  }
}
```

#### 2. **HTTP Cache-Aware Refresh**
```json
{
  "crawlerConfig": {
    "honorHttpCacheHeaders": true,        // Respect server Cache-Control headers
    "refreshInterval": "30 minutes",      // Fallback if no cache headers
    "dataFileCacheTTL": "1 hour"         // Local cache override
  }
}
```

#### 3. **On-Demand Refresh**
```json
{
  "crawlerConfig": {
    "refreshInterval": null,              // No automatic refresh
    "dataFileCacheTTL": "24 hours"       // Cache for a day until manual refresh
  }
}
```

### Content Caching Strategy
```json
{
  "crawlerConfig": {
    "dataFileCacheTTL": "2 hours",        // How long to cache discovered data files
    "htmlCacheTTL": "30 minutes",         // How long to cache HTML pages
    "honorHttpCacheHeaders": true,        // Respect Cache-Control headers
    "refreshInterval": "4 hours"          // How often to re-crawl
  }
}
```

### Cache Invalidation Strategy

#### HTTP Cache Headers Respect
```json
{
  "crawlerConfig": {
    "honorHttpCacheHeaders": true,        // Respect server Cache-Control headers
    "refreshInterval": "30 minutes",      // Fallback if no cache headers
    "dataFileCacheTTL": "1 hour",        // Local cache override
    "htmlCacheTTL": "30 minutes"         // HTML page cache duration
  }
}
```

**Note:** The crawler supports basic HTTP cache header handling. Advanced cache invalidation strategies like ETags, Last-Modified headers, and content hash validation are not currently implemented but may be added in future versions.

### Real-Time vs Batch Refresh Scenarios

#### Real-Time Data Portal
```json
{
  "name": "realtime_dashboard",
  "operand": {
    "storageType": "http",
    "baseUrl": "https://dashboard.company.com/live-data",
    "crawlerConfig": {
      "enabled": true,
      "refreshInterval": "5 minutes",      // Very frequent updates
      "htmlCacheTTL": "2 minutes",        // Short HTML cache
      "dataFileCacheTTL": "5 minutes",    // Short data cache
      "honorHttpCacheHeaders": false,     // Ignore server cache headers
      "maxPages": 10                      // Keep crawl scope small
    }
  }
}
```

#### Daily Batch Reporting
```json
{
  "name": "daily_reports",
  "operand": {
    "storageType": "http", 
    "baseUrl": "https://reports.company.com/daily",
    "crawlerConfig": {
      "enabled": true,
      "refreshInterval": "24 hours",       // Once per day
      "htmlCacheTTL": "12 hours",         // Long HTML cache
      "dataFileCacheTTL": "24 hours"      // Long data cache
    }
  }
}
```

#### Weekly Archive Processing
```json
{
  "name": "weekly_archives",
  "operand": {
    "storageType": "http",
    "baseUrl": "https://archive.company.com/weekly",
    "crawlerConfig": {
      "enabled": true,
      "refreshInterval": "7 days",         // Weekly refresh
      "dataFileCacheTTL": "14 days"       // Cache for 2 weeks
    }
  }
}
```

**Note:** Scheduled refresh (specific times, business hours, weekday-only) is not currently implemented. The crawler supports only interval-based refresh timing.
```

### Size and Performance Limits  
```json
{
  "crawlerConfig": {
    "maxHtmlSize": "10MB",               // Skip huge HTML pages
    "maxPages": 50,                      // Prevent runaway crawling
    "requestDelay": "1 second"           // Be polite to servers
  }
}
```

## Authentication for Protected Sites

### Basic Authentication
```json
{
  "storageType": "http",
  "storageConfig": {
    "baseUrl": "https://protected-reports.company.com",
    "authConfig": {
      "username": "${REPORTS_USER}",
      "password": "${REPORTS_PASS}"
    }
  },
  "crawlerConfig": {
    "enabled": true,
    "maxDepth": 2
  }
}
```

### API Token Authentication
```json
{
  "storageType": "http",
  "storageConfig": {
    "baseUrl": "https://api.company.com/reports",
    "authConfig": {
      "bearerToken": "${API_TOKEN}",
      "authHeaders": {
        "X-API-Version": "2024-01",
        "Accept": "text/html,application/json"
      }
    }
  }
}
```

### Dynamic Token Management
```json
{
  "storageConfig": {
    "authConfig": {
      "tokenCommand": "aws sts get-session-token --output text --query Credentials.SessionToken",
      "tokenFile": "/var/secrets/web-crawler-token.txt",
      "cacheTtl": 3600
    }
  }
}
```

## Real-World Use Cases

### Financial Data Aggregation
```json
{
  "name": "financial_data",
  "operand": {
    "storageType": "http",
    "baseUrl": "https://investor-relations.company.com",
    "crawlerConfig": {
      "enabled": true,
      "maxDepth": 2,
      "linkPattern": ".*\\/(quarterly|annual)\\/.*",
      "dataFilePattern": ".*\\.(xlsx|csv|pdf)$",
      "maxDataFileSize": "50MB",
      "htmlTableMinRows": 5,
      "refreshInterval": "1 day"
    }
  }
}
```

**Query the discovered data:**
```sql
-- Query extracted HTML tables
SELECT * FROM financial_data.quarterly_report__revenue 
WHERE quarter = 'Q1 2024';

-- Query downloaded Excel files  
SELECT * FROM financial_data.detailed_financials__summary
WHERE department = 'Engineering';
```

### Research Data Collection
```json
{
  "name": "research_portal",
  "operand": {
    "storageType": "http", 
    "baseUrl": "https://research.university.edu/datasets",
    "crawlerConfig": {
      "enabled": true,
      "maxDepth": 3,
      "linkPattern": ".*\\/(datasets|publications)\\/2024\\/.*",
      "dataFilePattern": ".*\\.(csv|json|parquet)$",
      "allowedDomains": ["research.university.edu", "data.university.edu"],
      "requestDelay": "3 seconds",
      "maxPages": 200
    }
  }
}
```

### Government Data Portals
```json
{
  "name": "gov_data",
  "operand": {
    "storageType": "http",
    "baseUrl": "https://data.gov/datasets/economic",
    "crawlerConfig": {
      "enabled": true,
      "maxDepth": 2, 
      "dataFilePattern": ".*\\.(csv|xlsx|json)$",
      "htmlTableMinRows": 10,
      "extensionSizeLimits": {
        "csv": "100MB",
        "xlsx": "200MB"
      },
      "refreshInterval": "12 hours"
    }
  }
}
```

## Advanced Scenarios

### Multi-Domain Crawling
```json
{
  "schemas": [
    {
      "name": "internal_reports",
      "operand": {
        "storageType": "http",
        "baseUrl": "https://internal-reports.company.com",
        "crawlerConfig": {
          "enabled": true,
          "followExternalLinks": false,
          "linkPattern": ".*\\/reports\\/.*"
        }
      }
    },
    {
      "name": "partner_data", 
      "operand": {
        "storageType": "http",
        "baseUrl": "https://partner-portal.vendor.com",
        "crawlerConfig": {
          "enabled": true,
          "allowedDomains": ["partner-portal.vendor.com"],
          "authConfig": {
            "apiKey": "${PARTNER_API_KEY}"
          }
        }
      }
    }
  ]
}
```

**Cross-domain analysis:**
```sql
SELECT 
  i.department,
  i.internal_revenue,
  p.partner_revenue,
  (i.internal_revenue + p.partner_revenue) as total_revenue
FROM internal_reports.monthly_summary i
JOIN partner_data.revenue_report p ON i.month = p.reporting_month;
```

### Scheduled Crawling with Materialized Views
```json
{
  "operand": {
    "crawlerConfig": {
      "refreshInterval": "6 hours"
    },
    "materializedViews": [{
      "view": "daily_metrics",
      "table": "daily_metrics_cache",
      "sql": "SELECT DATE(crawl_timestamp) as date, COUNT(*) as pages_crawled, SUM(file_count) as files_discovered FROM crawl_log GROUP BY DATE(crawl_timestamp)"
    }]
  }
}
```

## Monitoring and Troubleshooting

### Enable Crawler Logging
```bash
# Add to JVM arguments for detailed crawler logs
-Dorg.slf4j.simpleLogger.log.org.apache.calcite.adapter.file.converters.HtmlCrawler=DEBUG
```

### Common Issues and Solutions

**"Too many pages discovered"**
```json
{
  "crawlerConfig": {
    "maxPages": 50,              // Reduce page limit
    "maxDepth": 1,               // Reduce crawl depth
    "linkPattern": ".*\\/2024\\/.*"  // Be more specific
  }
}
```

**"Crawler timing out"**
```json
{
  "crawlerConfig": {
    "requestDelay": "5 seconds",     // Increase delay
    "maxHtmlSize": "5MB",           // Reduce HTML size limit
    "maxDataFileSize": "25MB"       // Reduce file size limit
  }
}
```

**"Authentication failing"**
```json
{
  "authConfig": {
    "tokenEnv": "API_TOKEN",        // Use environment variable
    "authHeaders": {
      "User-Agent": "DataCrawler/1.0"  // Some sites require specific user agent
    }
  }
}
```

**"No tables discovered"**
```json
{
  "crawlerConfig": {
    "htmlTableMinRows": 1,          // Lower table row threshold
    "generateTablesFromHtml": true  // Ensure HTML table extraction is enabled
  }
}
```

### Performance Optimization

**For large sites:**
```json
{
  "crawlerConfig": {
    "maxPages": 100,
    "requestDelay": "2 seconds",
    "dataFileCacheTTL": "4 hours",
    "htmlCacheTTL": "1 hour"
  }
}
```

**For frequently changing content:**
```json
{
  "crawlerConfig": {
    "refreshInterval": "30 minutes",
    "honorHttpCacheHeaders": false,
    "htmlCacheTTL": "15 minutes"
  }
}
```

## Integration Examples

### With Business Intelligence Tools
```sql
-- Create views for BI tool consumption
CREATE VIEW quarterly_financials AS
SELECT 
  quarter,
  revenue,
  expenses,
  profit_margin
FROM financial_data.quarterly_report__summary
WHERE year >= 2023;
```

### With Data Pipeline Tools
```java
// Automated data collection in Java
Properties props = new Properties();
props.setProperty("refreshInterval", "1 hour");
Connection conn = DriverManager.getConnection(
    "jdbc:apericio:model=/etc/crawler-config.json", props);

// Set up scheduled extraction
ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
scheduler.scheduleAtFixedRate(() -> {
    try (Statement stmt = conn.createStatement()) {
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM information_schema.tables");
        // Process discovered tables
    } catch (SQLException e) {
        logger.error("Crawl update failed", e);
    }
}, 0, 1, TimeUnit.HOURS);
```

The web crawling capability transforms the Apericio driver into a powerful automated data discovery and extraction tool, capable of turning any website into a queryable data source.
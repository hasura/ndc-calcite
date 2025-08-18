# SharePoint List JDBC Driver

A production-ready JDBC driver for SharePoint Lists that provides comprehensive SQL access using Apache Calcite with PostgreSQL-compatible syntax.

## 🚀 Quick Start

```java
// Load the driver
Class.forName("com.hasura.SharePointListDriver");

// Connect to SharePoint
String url = "jdbc:sharepoint:siteUrl='https://company.sharepoint.com/sites/mysite';" +
             "authType='CLIENT_CREDENTIALS';" +
             "clientId='your-client-id';" +
             "clientSecret='your-client-secret';" +
             "tenantId='your-tenant-id'";

Connection conn = DriverManager.getConnection(url);

// Query SharePoint lists with SQL
ResultSet rs = conn.createStatement().executeQuery(
    "SELECT title, due_date, priority FROM sharepoint.tasks WHERE status = 'Active'"
);
```

## ✨ Key Features

- **Full SQL Support**: Complete DDL (CREATE/DROP TABLE) and DML (SELECT/INSERT/UPDATE/DELETE) operations
- **Dual API Support**: Microsoft Graph API (cloud) and SharePoint REST API (on-premises)
- **Multiple Authentication**: Client Credentials, Certificates, Device Code, Managed Identity
- **Enterprise Ready**: Environment variables, auth proxy support, PostgreSQL compatibility
- **Attachment Handling**: SQL functions for binary attachment operations
- **Automatic Discovery**: SharePoint lists automatically mapped to SQL tables

## 📖 Documentation

### Getting Started
- **[Connection Guide](docs/connection.md)** - Connection URLs, authentication methods, and configuration
- **[SQL Operations](docs/sql-operations.md)** - Complete SQL syntax and examples
- **[Configuration](docs/configuration.md)** - Environment variables, parameters, and deployment options

### Advanced Features
- **[Dual API Support](docs/dual-api-support.md)** - Microsoft Graph API vs SharePoint REST API
- **[Attachment Functions](docs/attachments.md)** - Binary file handling through SQL functions
- **[Enterprise Features](docs/enterprise.md)** - Auth proxy, custom providers, and enterprise integration

### Reference & Deployment
- **[API Reference](docs/api-reference.md)** - Complete parameter and function reference
- **[Deployment Guide](docs/deployment.md)** - Building, testing, and releasing

## 🔧 Installation

### Maven
```xml
<dependency>
    <groupId>com.kenstott.components</groupId>
    <artifactId>sharepoint-list-jdbc-driver</artifactId>
    <version>1.0.1</version>
</dependency>
```

### Direct Download
Download the JAR with dependencies from [Maven Central](https://search.maven.org/artifact/com.kenstott.components/sharepoint-list-jdbc-driver).

## 🔐 Authentication Examples

### Client Credentials (Production)
```java
String url = "jdbc:sharepoint:siteUrl='https://company.sharepoint.com/sites/mysite';" +
             "authType='CLIENT_CREDENTIALS';" +
             "clientId='your-client-id';" +
             "clientSecret='your-client-secret';" +
             "tenantId='your-tenant-id'";
```

### Environment Variables
```bash
export SHAREPOINT_SITE_URL="https://company.sharepoint.com/sites/mysite"
export SHAREPOINT_CLIENT_ID="your-client-id"
export SHAREPOINT_CLIENT_SECRET="your-client-secret"
export SHAREPOINT_TENANT_ID="your-tenant-id"
```
```java
String url = "jdbc:sharepoint:"; // Uses environment variables
```

### Certificate Authentication
```java
String url = "jdbc:sharepoint:siteUrl='https://company.sharepoint.com/sites/mysite';" +
             "authType='CERTIFICATE';" +
             "clientId='your-client-id';" +
             "tenantId='your-tenant-id';" +
             "certificatePath='/path/to/cert.pfx';" +
             "certificatePassword='cert-password'";
```

## 📊 SQL Examples

### Basic Queries
```sql
-- Query SharePoint lists
SELECT * FROM sharepoint.tasks WHERE status = 'Active';

-- Insert new items
INSERT INTO sharepoint.tasks (title, description, priority)
VALUES ('Review docs', 'Review API documentation', 1);

-- Update existing items
UPDATE sharepoint.tasks 
SET status = 'Complete', completed_date = CURRENT_TIMESTAMP
WHERE id = '123';

-- Delete items
DELETE FROM sharepoint.tasks WHERE is_complete = true;
```

### Schema Management
```sql
-- Create new SharePoint list
CREATE TABLE sharepoint.project_tasks (
  title VARCHAR(255) NOT NULL,
  description VARCHAR(1000),
  due_date TIMESTAMP,
  priority INTEGER,
  is_complete BOOLEAN
);

-- Drop SharePoint list
DROP TABLE sharepoint.old_tasks;
```

### Attachment Handling
```sql
-- List attachments
SELECT filename, size FROM TABLE(get_attachments('tasks', '123'));

-- Count attachments
SELECT count_attachments('tasks', '123') as attachment_count;

-- Download attachment content (using PreparedStatement with byte[] result)
SELECT get_attachment_content('tasks', '123', 'document.pdf') as content;
```

## 🌐 API Support

The driver automatically selects the appropriate API:

- **SharePoint Online** (*.sharepoint.com) → Microsoft Graph API
- **On-Premises SharePoint** → SharePoint REST API

Force specific API:
```java
// Force Graph API
"apiType='graph'"

// Force REST API  
"apiType='rest'"
```

## 🏢 Enterprise Features

### Auth Proxy Integration
```java
Properties props = new Properties();
props.setProperty("siteUrl", "https://company.sharepoint.com/sites/mysite");
props.setProperty("authProxy.endpoint", "https://auth-proxy.company.com");
props.setProperty("authProxy.apiKey", System.getenv("AUTH_PROXY_KEY"));
Connection conn = DriverManager.getConnection("jdbc:sharepoint:", props);
```

### PostgreSQL Compatibility
```sql
-- PostgreSQL-compatible metadata
SELECT * FROM pg_catalog.pg_tables WHERE schemaname = 'sharepoint';

-- Information schema support
SELECT column_name, data_type FROM information_schema.columns 
WHERE table_name = 'tasks' AND table_schema = 'sharepoint';
```

## 🔗 Integration Examples

### Spring Boot
```java
@Bean
public DataSource sharePointDataSource() {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl("jdbc:sharepoint:");
    config.addDataSourceProperty("siteUrl", "${sharepoint.site.url}");
    config.addDataSourceProperty("authType", "CLIENT_CREDENTIALS");
    return new HikariDataSource(config);
}
```

### Apache Spark
```scala
val df = spark.read
  .format("jdbc")
  .option("url", "jdbc:sharepoint:")
  .option("dbtable", "sharepoint.tasks")
  .load()
```

## 🛠️ Building from Source

```bash
git clone https://github.com/your-repo/sharepoint-list-jdbc-driver.git
cd sharepoint-list-jdbc-driver
mvn clean package
```

JAR with dependencies: `target/sharepoint-list-jdbc-driver-1.0.1-jar-with-dependencies.jar`

## 📋 Requirements

- Java 11 or higher
- SharePoint Online or SharePoint Server 2019+
- Azure AD app registration with appropriate permissions

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

Apache License 2.0 - see [LICENSE](LICENSE) for details.

## 🆘 Support

- **Documentation**: Comprehensive guides in the [docs](docs/) directory
- **Issues**: Report bugs and request features on GitHub
- **Examples**: See the [examples](examples/) directory for complete applications

---

**Made with ❤️ for the SharePoint and Apache Calcite communities**
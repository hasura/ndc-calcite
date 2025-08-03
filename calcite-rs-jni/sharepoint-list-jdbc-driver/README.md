# SharePoint List JDBC Driver

A production-ready JDBC driver for SharePoint Lists that provides comprehensive SQL access using Apache Calcite with PostgreSQL-compatible syntax.

## Features

- **Full CRUD Support**: SELECT, INSERT, DELETE operations on SharePoint lists
- **DDL Support**: CREATE TABLE and DROP TABLE for managing SharePoint lists  
- **Microsoft Graph API**: Modern API with better performance and reliability
- **Multiple Authentication Methods**: Client Credentials, Username/Password, Certificate, Device Code, Managed Identity
- **Environment Variable Support**: Secure credential management for production deployments
- **PostgreSQL Compatibility**: PostgreSQL-compatible SQL syntax and metadata schemas
- **Automatic Schema Discovery**: Lists are automatically discovered as tables
- **Batch Operations**: Optimized bulk inserts using Graph API batching
- **Standard JDBC Interface**: Full JDBC 4.0 compliance for SharePoint data access

## Quick Start

### Connection URL Formats

**Direct Parameter Format:**
```java
String url = "jdbc:sharepoint:" +
    "siteUrl='https://yourcompany.sharepoint.com/sites/yoursite';" +
    "authType='CLIENT_CREDENTIALS';" +
    "clientId='your-azure-app-client-id';" +
    "clientSecret='your-azure-app-client-secret';" +
    "tenantId='your-azure-tenant-id'";
```

**PostgreSQL-Style Format:**
```java
String url = "jdbc:sharepoint://yourcompany.sharepoint.com/sites/yoursite/sharepoint";
Properties props = new Properties();
props.setProperty("authType", "CLIENT_CREDENTIALS");
props.setProperty("clientId", "your-azure-app-client-id");
props.setProperty("clientSecret", "your-azure-app-client-secret");
props.setProperty("tenantId", "your-azure-tenant-id");
```

**Environment Variables (Production):**
```bash
export SHAREPOINT_SITE_URL="https://yourcompany.sharepoint.com/sites/yoursite"
export SHAREPOINT_CLIENT_ID="your-azure-app-client-id"
export SHAREPOINT_CLIENT_SECRET="your-azure-app-client-secret"
export SHAREPOINT_TENANT_ID="your-azure-tenant-id"

String url = "jdbc:sharepoint:authType='CLIENT_CREDENTIALS'";
```

### Basic Usage

```java
import java.sql.*;

// Load the driver
Class.forName("com.hasura.sharepoint.SharePointListDriver");

// Create connection
Connection conn = DriverManager.getConnection(url, props);

// Query SharePoint lists
String sql = "SELECT title, due_date, priority FROM sharepoint.tasks WHERE status = 'Active'";
try (Statement stmt = conn.createStatement();
     ResultSet rs = stmt.executeQuery(sql)) {
    while (rs.next()) {
        System.out.println("Task: " + rs.getString("title"));
        System.out.println("Due: " + rs.getTimestamp("due_date"));
        System.out.println("Priority: " + rs.getInt("priority"));
    }
}

conn.close();
```

## Authentication Methods

### 1. Client Credentials (Service Principal) - Recommended for Production

```java
String url = "jdbc:sharepoint:" +
    "siteUrl='https://yourcompany.sharepoint.com/sites/yoursite';" +
    "authType='CLIENT_CREDENTIALS';" +
    "clientId='your-azure-app-client-id';" +
    "clientSecret='your-azure-app-client-secret';" +
    "tenantId='your-azure-tenant-id'";
```

### 2. Username/Password

```java
String url = "jdbc:sharepoint:" +
    "siteUrl='https://yourcompany.sharepoint.com/sites/yoursite';" +
    "authType='USERNAME_PASSWORD';" +
    "clientId='your-azure-app-client-id';" +
    "tenantId='your-azure-tenant-id';" +
    "username='user@yourcompany.com';" +
    "password='user-password'";
```

### 3. Certificate

```java
String url = "jdbc:sharepoint:" +
    "siteUrl='https://yourcompany.sharepoint.com/sites/yoursite';" +
    "authType='CERTIFICATE';" +
    "clientId='your-azure-app-client-id';" +
    "tenantId='your-azure-tenant-id';" +
    "certificatePath='/path/to/certificate.pfx';" +
    "certificatePassword='certificate-password';" +
    "thumbprint='certificate-thumbprint'";
```

### 4. Device Code (Interactive)

```java
String url = "jdbc:sharepoint:" +
    "siteUrl='https://yourcompany.sharepoint.com/sites/yoursite';" +
    "authType='DEVICE_CODE';" +
    "clientId='your-azure-app-client-id';" +
    "tenantId='your-azure-tenant-id'";
```

### 5. Managed Identity (Azure)

```java
String url = "jdbc:sharepoint:" +
    "siteUrl='https://yourcompany.sharepoint.com/sites/yoursite';" +
    "authType='MANAGED_IDENTITY';" +
    "clientId='optional-user-assigned-identity-client-id'";
```

## SQL Operations

### SELECT Operations

```sql
-- Query SharePoint lists
SELECT * FROM sharepoint.tasks WHERE status = 'Active';
SELECT title, due_date FROM sharepoint.project_tasks WHERE priority > 2;

-- PostgreSQL-compatible syntax
SELECT title, due_date::date FROM sharepoint.tasks 
WHERE created_date > CURRENT_TIMESTAMP - INTERVAL '7 days';

-- Complex queries with joins
SELECT t.title, p.project_name 
FROM sharepoint.tasks t
JOIN sharepoint.projects p ON t.project_id = p.id
WHERE t.status = 'In Progress';
```

### INSERT Operations

```sql
-- Single insert
INSERT INTO sharepoint.tasks (title, description, priority)
VALUES ('Review documentation', 'Review and update API docs', 1);

-- Batch insert (automatically optimized by Graph API)
INSERT INTO sharepoint.tasks (title, description, priority)
VALUES
  ('Task 1', 'Description 1', 1),
  ('Task 2', 'Description 2', 2),
  ('Task 3', 'Description 3', 3);
```

### DELETE Operations

```sql
-- Delete by ID
DELETE FROM sharepoint.tasks WHERE id = '123';

-- Delete multiple items
DELETE FROM sharepoint.tasks WHERE is_complete = true;
```

### DDL Operations

```sql
-- Create a new SharePoint list
CREATE TABLE sharepoint.project_tasks (
  title VARCHAR(255) NOT NULL,
  description VARCHAR(1000),
  assigned_to VARCHAR(255),
  due_date TIMESTAMP,
  priority INTEGER,
  is_complete BOOLEAN
);

-- Delete a SharePoint list
DROP TABLE sharepoint.project_tasks;
```

## PostgreSQL Metadata Support

The driver provides PostgreSQL-compatible system catalogs for metadata discovery:

```sql
-- List all SharePoint lists as tables
SELECT * FROM pg_catalog.pg_tables WHERE schemaname = 'sharepoint';

-- Get detailed column information
SELECT column_name, data_type, is_nullable, ordinal_position
FROM information_schema.columns
WHERE table_name = 'tasks' AND table_schema = 'sharepoint'
ORDER BY ordinal_position;

-- SharePoint-specific metadata
SELECT list_id, display_name, template_type, item_count
FROM pg_catalog.sharepoint_lists
WHERE template_type = 'TasksList';

-- List all schemas
SELECT * FROM information_schema.schemata;
```

## Environment Variables

For production deployments, use environment variables for secure credential management:

| Environment Variable | Description | Maps to Parameter |
|---------------------|-------------|-------------------|
| `SHAREPOINT_SITE_URL` | SharePoint site URL | `siteUrl` |
| `SHAREPOINT_CLIENT_ID` | Azure App Client ID | `clientId` |
| `SHAREPOINT_CLIENT_SECRET` | Azure App Client Secret | `clientSecret` |
| `SHAREPOINT_TENANT_ID` | Azure Tenant ID | `tenantId` |
| `SHAREPOINT_USERNAME` | Username | `username` |
| `SHAREPOINT_PASSWORD` | Password | `password` |
| `SHAREPOINT_AUTH_TYPE` | Authentication type | `authType` |

**Priority**: URL parameters > Properties > Environment variables

## Column Type Mapping

| SharePoint Type | SQL Type | Notes |
|----------------|----------|-------|
| Text, Note | VARCHAR | Single and multi-line text |
| Choice, MultiChoice | VARCHAR | Single and multiple selection |
| Number, Currency | DOUBLE | Decimal numbers |
| Integer, Counter | INTEGER | Whole numbers |
| Boolean | BOOLEAN | Yes/No fields |
| DateTime | TIMESTAMP | Date and time values |
| Lookup, User | VARCHAR | Displays title/name |
| URL, Hyperlink | VARCHAR | URL fields |

## Naming Convention

The driver automatically converts between SharePoint display names and SQL-friendly names:
- SharePoint list "Project Tasks" → SQL table `project_tasks`
- Column "Due Date" → SQL column `due_date`
- Column "Is Complete?" → SQL column `is_complete`

## Azure AD Configuration

1. **Register an app in Azure AD**
2. **Grant Microsoft Graph API permissions:**
   - `Sites.ReadWrite.All` (for full CRUD support)
   - `Sites.Manage.All` (for CREATE/DROP table support)
   - Or `Sites.Read.All` (for read-only access)
3. **Create a client secret** (for client credentials auth)
4. **Use the Application (client) ID, client secret, and Directory (tenant) ID** in your configuration

## Building

```bash
# Build the driver
mvn clean package

# The JAR with dependencies will be at:
# target/sharepoint-list-jdbc-driver-1.0.1-jar-with-dependencies.jar
```

## Testing

### Unit Tests
```bash
mvn test
```

### Integration Tests
The project includes comprehensive tests for all SharePoint adapter features:

- **SharePointListDriverTest**: Basic driver functionality and URL parsing
- **SharePointListDriverFacadeTest**: Comprehensive feature validation including authentication methods, CRUD operations, metadata support

#### Running Specific Test Suites
```bash
# Test basic driver functionality
mvn test -Dtest=SharePointListDriverTest

# Test comprehensive feature support
mvn test -Dtest=SharePointListDriverFacadeTest
```

#### Integration Tests with Real SharePoint
For integration tests with a real SharePoint instance:
1. Copy `local-properties.settings.sample` to `local-properties.settings`
2. Update with your SharePoint connection details
3. Ensure SharePoint is accessible and you have proper permissions
4. Run the tests - they will automatically detect and use the real connection

## Connection Examples

### Java Application
```java
import java.sql.*;
import java.util.Properties;

public class SharePointExample {
    public static void main(String[] args) throws SQLException {
        // Load driver
        try {
            Class.forName("com.hasura.sharepoint.SharePointListDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Driver not found", e);
        }
        
        // Set up connection
        String url = "jdbc:sharepoint:siteUrl='https://company.sharepoint.com/sites/hr'";
        Properties props = new Properties();
        props.setProperty("authType", "CLIENT_CREDENTIALS");
        props.setProperty("clientId", "your-client-id");
        props.setProperty("clientSecret", "your-client-secret");
        props.setProperty("tenantId", "your-tenant-id");
        
        try (Connection conn = DriverManager.getConnection(url, props)) {
            // Query employees list
            String sql = "SELECT employee_name, department, hire_date FROM sharepoint.employees WHERE status = 'Active'";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    System.out.printf("Employee: %s, Department: %s, Hired: %s%n",
                        rs.getString("employee_name"),
                        rs.getString("department"),
                        rs.getDate("hire_date"));
                }
            }
        }
    }
}
```

### Spring Boot Application
```java
@Configuration
public class SharePointConfig {
    
    @Bean
    public DataSource sharePointDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:sharepoint:");
        config.setUsername(""); // Not used
        config.setPassword(""); // Not used
        
        // Set SharePoint-specific properties
        config.addDataSourceProperty("siteUrl", "${sharepoint.site.url}");
        config.addDataSourceProperty("authType", "CLIENT_CREDENTIALS");
        config.addDataSourceProperty("clientId", "${sharepoint.client.id}");
        config.addDataSourceProperty("clientSecret", "${sharepoint.client.secret}");
        config.addDataSourceProperty("tenantId", "${sharepoint.tenant.id}");
        
        return new HikariDataSource(config);
    }
}
```

## Limitations

- **UPDATE**: Direct UPDATE statements are not supported (use DELETE + INSERT)
- **Complex Types**: No support for attachments or managed metadata
- **API Limits**: Subject to Microsoft Graph API throttling limits
- **Filtering**: Graph API OData filter syntax limitations apply
- **Name Conversion**: SharePoint lists/columns with null or empty display names use internal names as fallback

## Performance Tips

- Use batch INSERT operations for bulk data loads
- Include WHERE clauses to limit result sets
- Use specific column lists instead of SELECT *
- Consider SharePoint list view limits and pagination

## License

Same as the parent project (Apache License 2.0).
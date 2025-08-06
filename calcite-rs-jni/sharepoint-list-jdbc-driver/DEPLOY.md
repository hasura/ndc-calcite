# SharePoint List JDBC Driver - Deployment Guide

This document describes how to build, test, and deploy the SharePoint List JDBC Driver.

## Prerequisites

### Development Environment
- Java 11 or higher
- Maven 3.6 or higher
- Git

### For Releases
- Sonatype OSSRH account (for Maven Central deployment)
- GPG key for artifact signing
- Access to this repository

## Environment Variables

For deployment and release, set up these environment variables:

```bash
# Sonatype OSSRH credentials (for Maven Central)
export SONATYPE_USERNAME="your-sonatype-username"
export SONATYPE_PASSWORD="your-sonatype-password"

# GPG configuration (for artifact signing)
export GPG_PRIVATE_KEY_BASE64="$(cat ~/.gnupg/your-private-key.asc | base64 -w 0)"
export GPG_PASSPHRASE="your-gpg-passphrase"
export GPG_KEY_ID="your-gpg-key-id"
```

## Local Development

### Build
```bash
# Clean build
mvn clean compile

# Build with dependencies installed
mvn clean package

# Run tests
mvn test

# Run specific test
mvn test -Dtest=SharePointListDriverFacadeTest
```

### Install Dependencies
The project automatically installs local Calcite dependencies during the build process.

```bash
# This happens automatically during maven lifecycle:
# 1. calcite-core
# 2. calcite-sharepoint-list  
# 3. calcite-linq4j
# 4. calcite-babel
```

## Testing

### Unit Tests
```bash
# Run all tests
mvn test

# Run specific test classes
mvn test -Dtest=SharePointListDriverTest
mvn test -Dtest=SharePointListDriverFacadeTest
```

### Integration Tests
For integration tests with real SharePoint:

1. Copy the sample properties file:
   ```bash
   cp local-properties.settings.sample local-properties.settings
   ```

2. Edit `local-properties.settings` with your SharePoint connection details:
   ```properties
   sharepoint.siteUrl=https://yourcompany.sharepoint.com/sites/yoursite
   sharepoint.clientId=your-azure-app-client-id
   sharepoint.clientSecret=your-azure-app-client-secret
   sharepoint.tenantId=your-azure-tenant-id
   sharepoint.authType=CLIENT_CREDENTIALS
   ```

3. Run the tests:
   ```bash
   mvn test
   ```

## Building Artifacts

### Standard JAR
```bash
mvn clean package
```
Produces: `target/sharepoint-list-jdbc-driver-1.0.1.jar`

### JAR with Dependencies
```bash
mvn clean package
```
Also produces: `target/sharepoint-list-jdbc-driver-1.0.1-jar-with-dependencies.jar`

### Source and Javadoc JARs
```bash
mvn clean package -P release-sign-artifacts
```
Produces additional artifacts:
- `target/sharepoint-list-jdbc-driver-1.0.1-sources.jar`
- `target/sharepoint-list-jdbc-driver-1.0.1-javadoc.jar`

## Release Process

### Manual Release

1. **Prepare environment:**
   ```bash
   # Set up GPG
   ./setup-gpg.sh
   
   # Upload public key to key servers
   ./upload-gpg-key.sh
   ```

2. **Run release script:**
   ```bash
   ./release.sh
   ```

   This script will:
   - Verify you're on the right branch with no uncommitted changes
   - Prompt for confirmation of version numbers
   - Build and test the project
   - Create git tag
   - Deploy to Maven Central (if credentials are available)
   - Update to next development version

### Automated Release (CI/CD)

For automated releases in CI/CD environments:

```bash
# Release with auto-determined version
./release-auto.sh

# Release with specific version
./release-auto.sh 1.2.3
```

This script:
- Sets up GPG automatically
- Builds, tests, and deploys without prompts
- Creates git tags and pushes changes
- Uploads GPG key to key servers

## Deployment Targets

### Maven Central

Artifacts are deployed to Maven Central through Sonatype OSSRH:

**Group ID:** `com.kenstott.components`  
**Artifact ID:** `sharepoint-list-jdbc-driver`

Users can then include the driver in their projects:

```xml
<dependency>
    <groupId>com.kenstott.components</groupId>
    <artifactId>sharepoint-list-jdbc-driver</artifactId>
    <version>1.0.1</version>
</dependency>
```

### GitHub Releases

Git tags are automatically created during releases:
- Tag format: `v1.0.1`
- Release notes can be added manually on GitHub

## Artifact Verification

After deployment, verify the artifacts:

1. **Check Maven Central:**
   ```bash
   # Search for the artifact
   curl "https://search.maven.org/solrsearch/select?q=g:com.kenstott.components+AND+a:sharepoint-list-jdbc-driver"
   ```

2. **Verify GPG signatures:**
   ```bash
   # Download and verify
   gpg --verify sharepoint-list-jdbc-driver-1.0.1.jar.asc sharepoint-list-jdbc-driver-1.0.1.jar
   ```

3. **Test the published artifact:**
   ```java
   // Create a test project with the published dependency
   <dependency>
       <groupId>com.kenstott.components</groupId>
       <artifactId>sharepoint-list-jdbc-driver</artifactId>
       <version>1.0.1</version>
   </dependency>
   ```

## Troubleshooting

### Common Issues

1. **GPG signing fails:**
   ```bash
   # Verify GPG setup
   gpg --list-secret-keys
   
   # Test signing
   echo "test" | gpg --clearsign
   ```

2. **Maven Central deployment fails:**
   ```bash
   # Check credentials
   echo $SONATYPE_USERNAME
   echo $SONATYPE_PASSWORD
   
   # Verify settings.xml
   cat ~/.m2/settings.xml
   ```

3. **Tests fail:**
   ```bash
   # Check if local Calcite dependencies are installed
   ls ~/.m2/repository/org/apache/calcite/calcite-sharepoint-list/1.41.0-SNAPSHOT/
   
   # Rebuild dependencies
   mvn clean initialize
   ```

4. **Version conflicts:**
   ```bash
   # Check dependency tree
   mvn dependency:tree
   
   # Update to latest versions
   mvn versions:display-dependency-updates
   ```

## Version Management

### Version Scheme
- Release versions: `1.0.1`, `1.0.2`, etc.
- Development versions: `1.0.2-SNAPSHOT`

### Updating Versions
```bash
# Set specific version
mvn versions:set -DnewVersion=1.0.2

# Set next snapshot version
mvn versions:set -DnewVersion=1.0.2-SNAPSHOT

# Revert version changes
mvn versions:revert
```

## Security Considerations

### GPG Key Management
- Private keys should never be committed to version control
- Use base64-encoded private keys in CI/CD environment variables
- Regularly rotate GPG keys and update key servers

### Credential Management
- Use environment variables for sensitive data
- Never commit credentials to the repository
- Use secure CI/CD secret management

### Dependency Security
```bash
# Check for security vulnerabilities
mvn org.owasp:dependency-check-maven:check

# Update dependencies
mvn versions:display-dependency-updates
```

## Monitoring

### Build Status
Monitor the build status through:
- Maven build logs
- Test reports in `target/surefire-reports/`
- CI/CD pipeline status

### Deployment Status
- Check Sonatype OSSRH staging repositories
- Verify artifacts appear on Maven Central
- Monitor download statistics
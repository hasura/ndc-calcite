# Maven Central Deployment Guide

This guide explains how to deploy the Splunk JDBC Driver to Maven Central.

## Prerequisites

1. **Maven Central Account**
   - Sign up at https://central.sonatype.com
   - Namespace `com.kenstott.components` is already verified
   - Generate user tokens at https://central.sonatype.com/account

2. **GPG Key**
   - Run `./setup-gpg.sh` to create and configure GPG keys
   - Or manually create: `gpg --full-generate-key`
   - Upload to key servers: `gpg --keyserver hkp://keyserver.ubuntu.com --send-keys YOUR_KEY_ID`

3. **Maven Settings**
   - Copy `settings.xml.template` to `~/.m2/settings.xml`
   - Update with your credentials:
     - User token from https://central.sonatype.com/account
     - GPG passphrase

## One-time Setup

1. **Verify POM Configuration**
   ```bash
   mvn validate
   ```

2. **Test the Release Profile**
   ```bash
   mvn clean install -P release
   ```
   This should create:
   - `target/splunk-jdbc-driver-1.0.0.jar`
   - `target/splunk-jdbc-driver-1.0.0-sources.jar`
   - `target/splunk-jdbc-driver-1.0.0-javadoc.jar`
   - All artifacts should be signed (`.asc` files)

## Deployment Process

### Option 1: Automated Release (Recommended)
```bash
# Use the automated release script
./release-auto.sh

# This will:
# 1. Prompt for version increment type (major/minor/patch)
# 2. Update version automatically
# 3. Build and test
# 4. Deploy to Maven Central
# 5. Create git tags
# 6. Update to next SNAPSHOT version
```

### Option 2: Deploy Snapshot (for testing)
```bash
# Ensure version ends with -SNAPSHOT in pom.xml
mvn clean deploy
```

### Option 3: Manual Release

1. **Prepare the Release**
   ```bash
   # Ensure Calcite is built first
   cd ../calcite
   ./gradlew assemble
   
   # Return to driver directory
   cd ../splunk-jdbc-driver
   
   # Clean and test
   mvn clean test
   ```

2. **Deploy to Staging**
   ```bash
   mvn clean deploy -P release
   ```

3. **Release from Staging**
   
   The modern Central Portal uses automated publishing:
   - Artifacts are automatically validated upon upload
   - Publishing happens automatically if validation passes
   - Check status at https://central.sonatype.com/publishing/deployments
   - No manual staging/release steps required

## Quick Deploy Commands

```bash
# Full deployment in one command
mvn clean deploy -P release -Dgpg.passphrase="YOUR_PASSPHRASE"

# Skip tests for faster deployment (not recommended for production)
mvn clean deploy -P release -DskipTests
```

## Verify Deployment

After release (typically syncs within 30 minutes):

1. **Check Maven Central**
   - https://central.sonatype.com/artifact/com.kenstott.components/splunk-jdbc-driver
   - https://search.maven.org/artifact/com.kenstott.components/splunk-jdbc-driver
   - https://repo1.maven.org/maven2/com/kenstott/components/splunk-jdbc-driver/

2. **Test in a New Project**
   ```xml
   <dependency>
       <groupId>com.kenstott.components</groupId>
       <artifactId>splunk-jdbc-driver</artifactId>
       <version>1.0.0</version>
   </dependency>
   ```

## Version Management

### Automated Version Management

The project now includes automated version management:

1. **Using release-auto.sh** (Recommended)
   ```bash
   ./release-auto.sh
   # Select: 1) Patch, 2) Minor, 3) Major, 4) Custom
   ```

2. **Using Maven Release Plugin**
   ```bash
   # Prepare release (updates versions, creates tag)
   mvn release:prepare -DreleaseVersion=1.0.2 -DdevelopmentVersion=1.0.3-SNAPSHOT
   
   # Perform release (builds and deploys)
   mvn release:perform
   ```

3. **Manual Version Update**
   ```bash
   # Set specific version
   mvn versions:set -DnewVersion=1.0.2
   mvn versions:commit
   
   # Deploy
   mvn clean deploy -P release
   
   # Update to next snapshot
   mvn versions:set -DnewVersion=1.0.3-SNAPSHOT
   mvn versions:commit
   ```

### Version Numbering Convention
- MAJOR.MINOR.PATCH (e.g., 1.0.0)
- MAJOR: Breaking API changes
- MINOR: New features, backward compatible
- PATCH: Bug fixes

### GitHub Integration
The project is configured to:
- Create tags like `splunk-jdbc-driver-1.0.2`
- Link to the correct GitHub repository: https://github.com/hasura/ndc-calcite
- Not push changes automatically (use `git push origin main --tags`)

## Troubleshooting

### GPG Issues
- "gpg: signing failed: Inappropriate ioctl for device"
  ```bash
  export GPG_TTY=$(tty)
  ```

- "gpg: no default secret key"
  ```bash
  gpg --list-secret-keys
  # Use the key ID in settings.xml
  ```

### Deployment Failures
- Check https://central.sonatype.com/publishing/deployments for status
- Common issues:
  - Missing javadoc/sources
  - Invalid POM (missing required fields)
  - GPG signature failures
  - Network timeouts (retry)
  - Namespace not verified (already done for com.kenstott.components)

### Validation Errors
- Ensure all required POM elements are present:
  - name, description, url
  - licenses
  - developers
  - scm

## CI/CD Integration

For GitHub Actions deployment, add these secrets:
- `OSSRH_USERNAME`
- `OSSRH_PASSWORD`
- `MAVEN_GPG_PRIVATE_KEY` (exported with `gpg --export-secret-keys -a`)
- `MAVEN_GPG_PASSPHRASE`

Example workflow snippet:
```yaml
- name: Deploy to Maven Central
  env:
    MAVEN_USERNAME: ${{ secrets.OSSRH_USERNAME }}
    MAVEN_PASSWORD: ${{ secrets.OSSRH_PASSWORD }}
    MAVEN_GPG_PASSPHRASE: ${{ secrets.MAVEN_GPG_PASSPHRASE }}
  run: |
    echo "${{ secrets.MAVEN_GPG_PRIVATE_KEY }}" | gpg --import
    mvn clean deploy -P release -s settings.xml
```

## Support

- Maven Central Portal: https://central.sonatype.com
- Publishing Guide: https://central.sonatype.org/publish/publish-portal-maven/
- Requirements: https://central.sonatype.org/publish/requirements/
- API Documentation: https://central.sonatype.org/api-doc
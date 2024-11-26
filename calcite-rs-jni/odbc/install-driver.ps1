#Requires -RunAsAdministrator

param(
    [Parameter(Mandatory=$true)]
    [string]$dsnName,
    [Parameter(Mandatory=$true)]
    [string]$connectionString,
    [switch]$Force
)

function Write-Log {
    param([string]$Message)
    Write-Host $Message
}

# Define registry paths
$odbcDriversKey = "HKLM:\SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers"
$driverKey = "HKLM:\SOFTWARE\ODBC\ODBCINST.INI\DDN-ODBC-Driver"
$dsnKey = "HKLM:\SOFTWARE\ODBC\ODBC.INI\$dsnName"
$odbcDataSourcesKey = "HKLM:\SOFTWARE\ODBC\ODBC.INI\ODBC Data Sources"

# Create directory and copy files
$configDir = Join-Path $env:APPDATA "DDN_ODBC"
if (-not (Test-Path $configDir)) {
    New-Item -ItemType Directory -Path $configDir -Force | Out-Null
}

# Verify and copy required files
$requiredFiles = @("DDN-ODBC-Driver.dll", "sqlengine-1.0.0-jar-with-dependencies.jar")
foreach ($file in $requiredFiles) {
    if (-not (Test-Path $file)) {
        Write-Log "Required file not found: $file"
        exit 1
    }
}

try {
    Copy-Item -Path "DDN-ODBC-Driver.dll" -Destination $configDir -Force
    Copy-Item -Path "sqlengine-1.0.0-jar-with-dependencies.jar" -Destination $configDir -Force
    $driverPath = Join-Path $configDir "DDN-ODBC-Driver.dll"
}
catch {
    Write-Log "Error copying files: $_"
    exit 1
}

# Register driver in registry
try {
    if (-not (Test-Path $odbcDriversKey)) { New-Item -Path $odbcDriversKey -Force > $null }
    if (-not (Test-Path $driverKey)) { New-Item -Path $driverKey -Force > $null }
    
    New-ItemProperty -Path $odbcDriversKey -Name "DDN-ODBC-Driver" -Value "Installed" -PropertyType String -Force > $null

    $driverProperties = @{
        "Driver" = $driverPath
        "Setup" = $driverPath
        "APILevel" = "1"
        "ConnectFunctions" = "YYY"
        "DriverODBCVer" = "03.80"
        "FileUsage" = "0"
        "SQLLevel" = "1"
    }

    foreach ($prop in $driverProperties.GetEnumerator()) {
        New-ItemProperty -Path $driverKey -Name $prop.Key -Value $prop.Value -PropertyType String -Force > $null
    }

    Write-Log "Driver installed successfully at: $driverPath"
}
catch {
    Write-Log "Error setting up registry: $_"
    exit 1
}
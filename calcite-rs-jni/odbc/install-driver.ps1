#Requires -RunAsAdministrator

param(
    [Parameter(Mandatory=$true)]
    [string]$dsnName,
    [Parameter(Mandatory=$true)]
    [string]$Server,
    [Parameter(Mandatory=$true)]
    [string]$Port,
    [string]$Database = "graphql",
    [Parameter(Mandatory=$false)]
    [string]$UID,
    [Parameter(Mandatory=$false)]
    [SecureString]$PWD,
    [Parameter(Mandatory=$false)]
    [String]$Auth,
    [Parameter(Mandatory=$false)]
    [string]$Role,
    [string]$Timeout = "30",
    [string]$Encrypt = "false"
)

function Write-Log {
    param([string]$Message)
    Write-Host $Message
}

# Define registry paths
$odbcDriversKey = "HKLM:\SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers"
$driverKeyARM64 = "HKLM:\SOFTWARE\ODBC\ODBCINST.INI\DDN-ODBC-Driver-ARM64"
$driverKeyx64 = "HKLM:\SOFTWARE\ODBC\ODBCINST.INI\DDN-ODBC-Driver-x64"
$dsnKeyx64 = "HKLM:\SOFTWARE\ODBC\ODBC.INI\$dsnNamex64"
$dsnKeyARM64 = "HKLM:\SOFTWARE\ODBC\ODBC.INI\$dsnNameARM64"
$dsnNameARM64 = "${dsnName}ARM64"
$dsnNamex64 = "${dsnName}x64"
$odbcDataSourcesKey = "HKLM:\SOFTWARE\ODBC\ODBC.INI\ODBC Data Sources"
$odbcDsnPath = "HKLM:\SOFTWARE\ODBC\ODBC.INI"

# Create directory and copy files
$configDir = Join-Path $env:APPDATA "DDN_ODBC"
if (-not (Test-Path $configDir)) {
    New-Item -ItemType Directory -Path $configDir -Force | Out-Null
    New-Item -ItemType Directory -Path "$configDir\ARM64" -Force | Out-Null
    New-Item -ItemType Directory -Path "$configDir\x64" -Force | Out-Null
}

# Verify and copy required files
$requiredFiles = @("DDN-ODBC-Driver.dll", "jni-arrow-1.0.0-jar-with-dependencies.jar")

foreach ($file in $requiredFiles) {
    if (-not (Test-Path ARM64\$file)) {
        Write-Log "Required file not found: $file"
        exit 1
    }
}
foreach ($file in $requiredFiles) {
    if (-not (Test-Path x64\$file)) {
        Write-Log "Required file not found: $file"
        exit 1
    }
}

try {
    Copy-Item -Path "ARM64\DDN-ODBC-Driver.dll" -Destination "$configDir\ARM64\" -Force
    Copy-Item -Path "ARM64\jni-arrow-1.0.0-jar-with-dependencies.jar" -Destination "$configDir\ARM64\" -Force
    $driverPathARM64 = Join-Path "$configDir\ARM64" "DDN-ODBC-Driver.dll"
}
catch {
    Write-Log "Error copying files: $_"
    exit 1
}

try {
    Copy-Item -Path "x64\DDN-ODBC-Driver.dll" -Destination "$configDir\x64\" -Force
    Copy-Item -Path "x64\jni-arrow-1.0.0-jar-with-dependencies.jar" -Destination "$configDir\x64\" -Force
    $driverPathx64 = Join-Path "$configDir\x64" "DDN-ODBC-Driver.dll"
}
catch {
    Write-Log "Error copying files: $_"
    exit 1
}

# Register driver in registry
try {
    if (-not (Test-Path $odbcDriversKey)) { New-Item -Path $odbcDriversKey -Force > $null }
    if (-not (Test-Path $driverKeyARM64)) { New-Item -Path $driverKeyARM64 -Force > $null }
    if (-not (Test-Path $driverKeyx64)) { New-Item -Path $driverKeyx64 -Force > $null }
    if (-not (Test-Path "${odbcDsnPath}\${dsnNameARM64}")) { New-Item -Path "${odbcDsnPath}\${dsnNameARM64}" -Force > $null }
    if (-not (Test-Path "${odbcDsnPath}\${dsnNamex64}")) { New-Item -Path "${odbcDsnPath}\${dsnNamex64}" -Force > $null }

    New-ItemProperty -Path $odbcDriversKey -Name "DDN-ODBC-Driver-ARM64" -Value "Installed" -PropertyType String -Force > $null
    New-ItemProperty -Path $odbcDriversKey -Name "DDN-ODBC-Driver-x64" -Value "Installed" -PropertyType String -Force > $null

    $driverProperties = @{
        "Driver" = $driverPathARM64
        "Setup" = $driverPathARM64
        "APILevel" = "2"
        "ConnectFunctions" = "YYY"
        "DriverODBCVer" = "03.80"
        "FileUsage" = "0"
        "SQLLevel" = "3"
    }

    foreach ($prop in $driverProperties.GetEnumerator()) {
        New-ItemProperty -Path $driverKeyARM64 -Name $prop.Key -Value $prop.Value -PropertyType String -Force > $null
    }
    New-ItemProperty -Path $odbcDataSourcesKey -Name $dsnNameARM64 -Value "DDN-ODBC-Driver-ARM64" -PropertyType String -Force > $null
    New-ItemProperty -Path "${odbcDsnPath}\${dsnNameARM64}" -Name "Driver" -Value "DDN-ODBC-Driver-ARM64" -PropertyType String -Force > $null
    New-ItemProperty -Path "${odbcDsnPath}\${dsnNameARM64}" -Name "Server" -Value $Server -PropertyType String -Force > $null
    New-ItemProperty -Path "${odbcDsnPath}\${dsnNameARM64}" -Name "Port" -Value $Port -PropertyType String -Force > $null
    New-ItemProperty -Path "${odbcDsnPath}\${dsnNameARM64}" -Name "Database" -Value $Database -PropertyType String -Force > $null
    New-ItemProperty -Path "${odbcDsnPath}\${dsnNameARM64}" -Name "Encrypt" -Value $Encrypt -PropertyType String -Force > $null
    New-ItemProperty -Path "${odbcDsnPath}\${dsnNameARM64}" -Name "Timeout" -Value $Timeout -PropertyType String -Force > $null
    if ($Role)
    {
        New-ItemProperty -Path "${odbcDsnPath}\${dsnNameARM64}" -Name "Role" -Value $Role -PropertyType String -Force > $null
    }
    if ($UID)
    {
        New-ItemProperty -Path "${odbcDsnPath}\${dsnNameARM64}" -Name "UID" -Value $UID -PropertyType String -Force > $null
    }
    if ($PWD)
    {
        New-ItemProperty -Path "${odbcDsnPath}\${dsnNameARM64}" -Name "PWD" -Value $PWD -PropertyType String -Force > $null
    }
    if ($Auth)
    {
        New-ItemProperty -Path "${odbcDsnPath}\${dsnNameARM64}" -Name "Auth" -Value $Auth -PropertyType String -Force > $null
    }

    $driverProperties = @{
        "Driver" = $driverPathx64
        "Setup" = $driverPathx64
        "APILevel" = "2"
        "ConnectFunctions" = "YYY"
        "DriverODBCVer" = "03.80"
        "FileUsage" = "0"
        "SQLLevel" = "3"
    }

    foreach ($prop in $driverProperties.GetEnumerator()) {
        New-ItemProperty -Path $driverKeyx64 -Name $prop.Key -Value $prop.Value -PropertyType String -Force > $null
    }
    New-ItemProperty -Path $odbcDataSourcesKey -Name $dsnNamex64 -Value "DDN-ODBC-Driver-x64" -PropertyType String -Force > $null
    New-ItemProperty -Path "${odbcDsnPath}\${dsnNamex64}" -Name "Driver" -Value "DDN-ODBC-Driver-x64" -PropertyType String -Force > $null
    New-ItemProperty -Path "${odbcDsnPath}\${dsnNamex64}" -Name "Server" -Value $Server -PropertyType String -Force > $null
    New-ItemProperty -Path "${odbcDsnPath}\${dsnNamex64}" -Name "Port" -Value $Port -PropertyType String -Force > $null
    New-ItemProperty -Path "${odbcDsnPath}\${dsnNamex64}" -Name "Database" -Value $Database -PropertyType String -Force > $null
    New-ItemProperty -Path "${odbcDsnPath}\${dsnNamex64}" -Name "Encrypt" -Value $Encrypt -PropertyType String -Force > $null
    New-ItemProperty -Path "${odbcDsnPath}\${dsnNamex64}" -Name "Timeout" -Value $Timeout -PropertyType String -Force > $null
    if ($Role)
    {
        New-ItemProperty -Path "${odbcDsnPath}\${dsnNamex64}" -Name "Role" -Value $Role -PropertyType String -Force > $null
    }
    if ($UID)
    {
        New-ItemProperty -Path "${odbcDsnPath}\${dsnNamex64}" -Name "UID" -Value $UID -PropertyType String -Force > $null
    }
    if ($PWD)
    {
        New-ItemProperty -Path "${odbcDsnPath}\${dsnNamex64}" -Name "PWD" -Value $PWD -PropertyType String -Force > $null
    }
    if ($Auth)
    {
        New-ItemProperty -Path "${odbcDsnPath}\${dsnNamex64}" -Name "Auth" -Value $Auth -PropertyType String -Force > $null
    }
    Write-Log "Driver installed successfully at: $driverPath"
}
catch {
    Write-Log "Error setting up registry: $_"
    exit 1
}
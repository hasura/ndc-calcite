param (
    [switch]$Clean = $false  # By default, do not run 'clean'
)

$ErrorActionPreference = 'Stop'
$startTime = Get-Date
$buildResults = @()
$projectDir = "DDN-ODBC-Driver"  # Add project directory name

# Verify VCPKG_ROOT is set
if (-not $env:VCPKG_ROOT) {
    throw "VCPKG_ROOT environment variable is not set"
}

function Write-Step {
    param([string]$message)
    Write-Host "`n=== $message ===" -ForegroundColor Cyan
}

function Format-Duration {
    param([TimeSpan]$duration)
    return "{0:mm}:{0:ss}.{0:fff}" -f $duration
}

function Remove-MacOSMetadata {
    param([string]$path)
    Write-Host "Cleaning macOS metadata from $path"
    Write-Host "Looking for files matching: $path\._*"
    $macFiles = Get-ChildItem -Path $path -Filter "._*" -Recurse -Force -ErrorAction SilentlyContinue
    if ($macFiles) {
        Write-Host "Found metadata files:"
        $macFiles | ForEach-Object { Write-Host "  $_" }
        $macFiles | Remove-Item -Force -ErrorAction SilentlyContinue
    } else {
        Write-Host "No metadata files found"
    }
}

function Safe-Copy {
    param(
        [string]$sourcePath,
        [string]$destPath
    )
    # Completely skip if it's a Mac metadata file
    if ($sourcePath -like "*._*") {
        Write-Host "  Skipping macOS metadata file: $sourcePath"
        return $true # Return success
    }

    if (Test-Path $sourcePath) {
        Write-Host "  Source exists, copying..."
        Copy-Item -Path $sourcePath -Destination $destPath -Force
        return $true
    } else {
        throw "Could not find file '$sourcePath'"
    }
}

try {
    if ($Clean) {
        # Clean
        Write-Step "Cleaning solution"
        $stepStart = Get-Date
        Remove-MacOSMetadata -path "."
        .\clean.ps1 | Tee-Object -FilePath "build-log.txt" -Append
        $buildResults += @{ Step = "Clean"; Duration = (Get-Date) - $stepStart; Status = "Success" }
    }

    # Build for each configuration and platform
    foreach ($configuration in @("Debug", "Release")) {
        foreach ($platform in @("ARM64", "X64")) {
            Write-Step "Building DDN-ODBC-Driver for $platform $configuration"
            $stepStart = Get-Date

            Remove-MacOSMetadata -path $projectDir

            # Configure CMake from project directory
            $buildDir = "..\build\$platform\$configuration"
            Push-Location $projectDir

            # Add DEBUG define for Debug configuration
            $defineFlags = if ($configuration -eq "Debug") { "-DDEBUG=1" } else { "" }

            cmake -B $buildDir -A $platform -DCMAKE_BUILD_TYPE=$configuration `
                  -DCMAKE_TOOLCHAIN_FILE="$env:VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake" `
                  $defineFlags `
                  | Tee-Object -FilePath "..\build-log.txt" -Append
            if ($LASTEXITCODE -ne 0) {
                Pop-Location
                throw "CMake configuration for $platform $configuration failed"
            }

            # Build
            cmake --build $buildDir --config $configuration | Tee-Object -FilePath "..\build-log.txt" -Append
            if ($LASTEXITCODE -ne 0) {
                Pop-Location
                throw "CMake build for $platform $configuration failed"
            }
            Pop-Location

            $buildResults += @{ Step = "DDN-ODBC-Driver $platform $configuration"; Duration = (Get-Date) - $stepStart; Status = "Success" }
        }

        # Copy deployment files for this configuration
        Write-Step "Copying deployment files for $configuration"
        $stepStart = Get-Date

        # Clean any Mac metadata before copying
        Remove-MacOSMetadata -path "build"

        # Create bin\$configuration directory and copy install-driver.ps1 there
        $configDir = "bin\$configuration"
        New-Item -ItemType Directory -Force -Path $configDir | Out-Null
        Safe-Copy -sourcePath "install-driver.ps1" -destPath $configDir

        foreach ($platform in @("ARM64", "x64")) {
            $destPath = "bin\$configuration\$platform"
            $testPath = "$destPath\test"
            New-Item -ItemType Directory -Force -Path $testPath | Out-Null

            # Copy driver files (adjust paths for build directory change)
            Safe-Copy -sourcePath "build\$platform\$configuration\bin\DDN-ODBC-Driver.dll" -destPath $destPath
            Safe-Copy -sourcePath "build\$platform\$configuration\bin\jni-arrow-1.0.0-jar-with-dependencies.jar" -destPath $destPath
        }

        $buildResults += @{ Step = "Copy Files $configuration"; Duration = (Get-Date) - $stepStart; Status = "Success" }
    }

    # Verify outputs
    Write-Step "Verifying build outputs"
    $stepStart = Get-Date

    # Clean any Mac metadata before verification
    Remove-MacOSMetadata -path "bin"

    $expectedOutputs = @()
    foreach ($configuration in @("Debug", "Release")) {
        $expectedOutputs += "bin\$configuration\install-driver.ps1"
        foreach ($platform in @("ARM64", "x64")) {
            $expectedOutputs += "bin\$configuration\$platform\DDN-ODBC-Driver.dll"
            $expectedOutputs += "bin\$configuration\$platform\jni-arrow-1.0.0-jar-with-dependencies.jar"
        }
    }

    $missingFiles = $expectedOutputs | Where-Object { -not (Test-Path $_) }
    if ($missingFiles) {
        throw "Missing expected outputs:`n$($missingFiles -join "`n")"
    }
    $buildResults += @{ Step = "Verification"; Duration = (Get-Date) - $stepStart; Status = "Success" }

    # Create MSI installers
    Write-Step "Creating MSI installers"
    $stepStart = Get-Date

    # Clean any Mac metadata before MSI creation
    Remove-MacOSMetadata -path "bin"

    .\make_msi.ps1 | Tee-Object -FilePath "build-log.txt" -Append
    if ($LASTEXITCODE -ne 0) {
        throw "MSI creation failed"
    }
    $buildResults += @{ Step = "Create MSI"; Duration = (Get-Date) - $stepStart; Status = "Success" }

    if ($Clean) {
        # Clean metadata
        Write-Step "Cleaning metadata"
        $stepStart = Get-Date
        .\clean_metadata.ps1 | Tee-Object -FilePath "build-log.txt" -Append
        $buildResults += @{ Step = "Clean Metadata"; Duration = (Get-Date) - $stepStart; Status = "Success" }
    }

    # Print summary
    $totalDuration = (Get-Date) - $startTime
    Write-Host "`n=== Build Summary ===" -ForegroundColor Green
    Write-Host "Total Duration: $(Format-Duration $totalDuration)" -ForegroundColor Green
    foreach ($configuration in @("Debug", "Release")) {
        Write-Host "`nOutputs for ${configuration}:"
        Write-Host "  - bin\$configuration\ARM64"
        Write-Host "  - bin\$configuration\x64"
        Write-Host "  - installer\$configuration\ARM64"
        Write-Host "  - installer\$configuration\x64"
    }
} catch {
    Write-Host "`n=== Build Failed ===" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    foreach ($result in $buildResults) {
        Write-Host "$($result.Step): " -NoNewline
        if ($result.Status -eq "Success") {
            Write-Host "$($result.Status)" -ForegroundColor Green
        } else {
            Write-Host "$($result.Status)" -ForegroundColor Red
        }
    }
    exit 1
}
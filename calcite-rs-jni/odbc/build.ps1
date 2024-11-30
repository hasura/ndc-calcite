param (
    [switch]$Clean = $false  # By default, do not run 'clean'
)

$ErrorActionPreference = 'Stop'
$startTime = Get-Date
$buildResults = @()
$configuration = "Debug"
$projectDir = "DDN-ODBC-Driver"  # Add project directory name

# Verify VCPKG_ROOT is set
if (-not $env:VCPKG_ROOT) {
    throw "VCPKG_ROOT environment variable is not set"
}

function Write-Step { param([string]$message) Write-Host "`n=== $message ===" -ForegroundColor Cyan }
function Format-Duration { param([TimeSpan]$duration) return "{0:mm}:{0:ss}.{0:fff}" -f $duration }

try {
    if ($Clean) {
        # Clean
        Write-Step "Cleaning solution"
        $stepStart = Get-Date
        .\clean.ps1 | Tee-Object -FilePath "build-log.txt" -Append
        $buildResults += @{ Step = "Clean"; Duration = (Get-Date) - $stepStart; Status = "Success" }
    }

    # Build Drivers for each platform
    foreach ($platform in @("ARM64", "X64")) {
        Write-Step "Building DDN-ODBC-Driver for $platform"
        $stepStart = Get-Date

        # Configure CMake from project directory
        $buildDir = "..\build\$platform\$configuration"
        Push-Location $projectDir
        cmake -B $buildDir -A $platform -DCMAKE_BUILD_TYPE=$configuration `
              -DCMAKE_TOOLCHAIN_FILE="$env:VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake" `
              | Tee-Object -FilePath "..\build-log.txt" -Append
        if ($LASTEXITCODE -ne 0) {
            Pop-Location
            throw "CMake configuration for $platform failed"
        }

        # Build
        cmake --build $buildDir --config $configuration | Tee-Object -FilePath "..\build-log.txt" -Append
        if ($LASTEXITCODE -ne 0) {
            Pop-Location
            throw "CMake build for $platform failed"
        }
        Pop-Location

        $buildResults += @{ Step = "DDN-ODBC-Driver $platform"; Duration = (Get-Date) - $stepStart; Status = "Success" }
    }

    # Copy deployment files to platform-specific folders
    Write-Step "Copying deployment files"
    $stepStart = Get-Date

    foreach ($platform in @("ARM64", "x64")) {
        $destPath = "bin\$configuration\$platform"
        $testPath = "$destPath\test"
        New-Item -ItemType Directory -Force -Path $testPath | Out-Null

        # Copy driver files (adjust paths for build directory change)
        Copy-Item -Path "build\$platform\$configuration\bin\DDN-ODBC-Driver.dll" -Destination $destPath -Force
        Copy-Item -Path "build\$platform\$configuration\bin\jni-arrow-1.0.0-jar-with-dependencies.jar" -Destination $destPath -Force
        Copy-Item -Path "install-driver.ps1" -Destination $destPath -Force
    }

    $buildResults += @{ Step = "Copy Files"; Duration = (Get-Date) - $stepStart; Status = "Success" }

    # Verify outputs
    Write-Step "Verifying build outputs"
    $stepStart = Get-Date
    $expectedOutputs = foreach ($platform in @("ARM64", "x64")) {
        "bin\$configuration\$platform\DDN-ODBC-Driver.dll"
        "bin\$configuration\$platform\jni-arrow-1.0.0-jar-with-dependencies.jar"
        "bin\$configuration\$platform\install-driver.ps1"
    }

    $missingFiles = $expectedOutputs | Where-Object { -not (Test-Path $_) }
    if ($missingFiles) { throw "Missing expected outputs:`n$($missingFiles -join "`n")" }
    $buildResults += @{ Step = "Verification"; Duration = (Get-Date) - $stepStart; Status = "Success" }

    if ($Clean)
    {
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
    Write-Host "`nTest outputs in:"
    Write-Host "  - bin\$configuration\ARM64"
    Write-Host "  - bin\$configuration\x64"
    Write-Host "`nRelease outputs in:"
    Write-Host "  - bin\ARM64\$configuration"
    Write-Host "  - bin\x64\$configuration"
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
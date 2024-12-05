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
    if ($sourcePath -like "*._*") {
        Write-Host "  Skipping macOS metadata file: $sourcePath"
        return $true
    }

    if (Test-Path $sourcePath) {
        Write-Host "  Source exists, copying..."
        Copy-Item -Path $sourcePath -Destination $destPath -Force
        return $true
    } else {
        throw "Could not find file '$sourcePath'"
    }
}

# Check if Inno Setup is installed
function Test-InnoSetup {
    $innoPath = "${env:ProgramFiles(x86)}\Inno Setup 6\ISCC.exe"
    if (-not (Test-Path $innoPath)) {
        Write-Host "Inno Setup not found at: $innoPath" -ForegroundColor Red
        Write-Host "Please install Inno Setup 6 from: https://jrsoftware.org/isdl.php" -ForegroundColor Yellow
        exit 1
    }
    return $innoPath
}

try {
    $innoCompiler = Test-InnoSetup

    if ($Clean) {
        Write-Step "Cleaning solution"
        $stepStart = Get-Date
        Remove-MacOSMetadata -path "."
        .\clean.ps1 | Tee-Object -FilePath "build-log.txt" -Append
        $buildResults += @{ Step = "Clean"; Duration = (Get-Date) - $stepStart; Status = "Success" }
    }

    foreach ($configuration in @("Debug", "Release")) {
        foreach ($platform in @("ARM64", "X64")) {
            Write-Step "Building DDN-ODBC-Driver for $platform $configuration"
            $stepStart = Get-Date

            Remove-MacOSMetadata -path $projectDir

            # Configure CMake from project directory
            $buildDir = "..\build\$platform\$configuration"
            Push-Location $projectDir

            $defineFlags = if ($configuration -eq "Debug") { "-DDEBUG=1" } else { "" }

            cmake -B $buildDir -A $platform -DCMAKE_BUILD_TYPE=$configuration `
                  -DCMAKE_TOOLCHAIN_FILE="$env:VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake" `
                  $defineFlags `
                  | Tee-Object -FilePath "..\build-log.txt" -Append
            if ($LASTEXITCODE -ne 0) {
                Pop-Location
                throw "CMake configuration for $platform $configuration failed"
            }

            cmake --build $buildDir --config $configuration | Tee-Object -FilePath "..\build-log.txt" -Append
            if ($LASTEXITCODE -ne 0) {
                Pop-Location
                throw "CMake build for $platform $configuration failed"
            }
            Pop-Location

            $buildResults += @{ Step = "DDN-ODBC-Driver $platform $configuration"; Duration = (Get-Date) - $stepStart; Status = "Success" }
        }

        Write-Step "Copying deployment files for $configuration"
        $stepStart = Get-Date

        Remove-MacOSMetadata -path "build"

        $configDir = "bin\$configuration"
        New-Item -ItemType Directory -Force -Path $configDir | Out-Null
        Safe-Copy -sourcePath "install-driver.ps1" -destPath $configDir

        foreach ($platform in @("ARM64", "x64")) {
            $destPath = "bin\$configuration\$platform"
            $testPath = "$destPath\test"
            New-Item -ItemType Directory -Force -Path $testPath | Out-Null

            Safe-Copy -sourcePath "build\$platform\$configuration\bin\DDN-ODBC-Driver.dll" -destPath $destPath
            Safe-Copy -sourcePath "build\$platform\$configuration\bin\jni-arrow-1.0.0-jar-with-dependencies.jar" -destPath $destPath
        }

        $buildResults += @{ Step = "Copy Files $configuration"; Duration = (Get-Date) - $stepStart; Status = "Success" }
    }

    # Create Inno Setup script
    Write-Step "Creating Inno Setup script"
    $stepStart = Get-Date

    $innoScript = @'
#define MyAppName "DDN ODBC Driver"
#define MyAppVersion "1.0.0"
#define MyAppPublisher "DDN"

[Setup]
AppId={{31B2AA47-ADD4-4EF3-EF72FAA7FB46}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
DefaultDirName={autopf}\DDN\ODBC Driver
DefaultGroupName={#MyAppName}
OutputBaseFilename=DDN-ODBC-Driver-Setup
Compression=lzma
SolidCompression=yes
ArchitecturesInstallIn64BitMode=x64
PrivilegesRequired=admin

[Files]
; x64 files
Source: "bin\Release\x64\DDN-ODBC-Driver.dll"; DestDir: "{app}\x64"; Flags: ignoreversion
Source: "bin\Release\x64\jni-arrow-1.0.0-jar-with-dependencies.jar"; DestDir: "{app}\x64"; Flags: ignoreversion

; ARM64 files
Source: "bin\Release\ARM64\DDN-ODBC-Driver.dll"; DestDir: "{app}\ARM64"; Flags: ignoreversion
Source: "bin\Release\ARM64\jni-arrow-1.0.0-jar-with-dependencies.jar"; DestDir: "{app}\ARM64"; Flags: ignoreversion

; Install script
Source: "bin\Release\install-driver.ps1"; DestDir: "{app}"; Flags: ignoreversion

[Code]
var
  DsnPage: TInputQueryWizardPage;
  
procedure InitializeWizard;
begin
  DsnPage := CreateInputQueryPage(wpSelectDir,
    'DSN Information', 
    'Enter the connection details for the ODBC DSN.',
    'Please specify the following information:');
    
  // Add fields with more compact spacing (y-position decreased from 24 to 20)
  DsnPage.Add('DSN Name:', False);
  DsnPage.Add('Server:', False);
  DsnPage.Add('Port:', False);
  DsnPage.Add('Database:', False);
  DsnPage.Add('Username (optional):', False);
  DsnPage.Add('Password (optional):', True);
  DsnPage.Add('Role (optional):', False);
  DsnPage.Add('Auth Method (optional):', False);

  // Set default values
  DsnPage.Values[0] := 'DDN_ODBC';
  DsnPage.Values[3] := 'graphql';

  // Adjust the spacing for all edit boxes
  for var i := 0 to DsnPage.EditorCount - 1 do
  begin
    DsnPage.Edits[i].Top := 60 + (i * 30);  // Adjust spacing between fields
    DsnPage.PromptLabels[i].Top := DsnPage.Edits[i].Top + 2;  // Align labels with edit boxes
  end;

end;

function NextButtonClick(CurPageID: Integer): Boolean;
begin
  Result := True;

  if CurPageID = DsnPage.ID then
  begin
    if DsnPage.Values[0] = '' then
    begin
      MsgBox('Please enter a DSN Name.', mbError, MB_OK);
      Result := False;
    end
    else if DsnPage.Values[1] = '' then
    begin
      MsgBox('Please enter a Server address.', mbError, MB_OK);
      Result := False;
    end
    else if DsnPage.Values[2] = '' then
    begin
      MsgBox('Please enter a Port number.', mbError, MB_OK);
      Result := False;
    end;
  end;
end;

[Run]
Filename: "powershell.exe"; \
  Parameters: "-ExecutionPolicy Bypass -File ""{app}\install-driver.ps1"" -dsnName ""{code:GetDsnName}"" -Server ""{code:GetServer}"" -Port ""{code:GetPort}"" -Database ""{code:GetDatabase}"" -UID ""{code:GetUsername}"" -PWD ""{code:GetPassword}"" -Role ""{code:GetRole}"" -Auth ""{code:GetAuth}"""; \
  Flags: runhidden waituntilterminated; \
  StatusMsg: "Configuring ODBC DSN..."

[Code]
function GetDsnName(Param: String): String;
begin
  Result := DsnPage.Values[0];
end;

function GetServer(Param: String): String;
begin
  Result := DsnPage.Values[1];
end;

function GetPort(Param: String): String;
begin
  Result := DsnPage.Values[2];
end;

function GetDatabase(Param: String): String;
begin
  Result := DsnPage.Values[3];
end;

function GetUsername(Param: String): String;
begin
  Result := DsnPage.Values[4];
end;

function GetPassword(Param: String): String;
begin
  Result := DsnPage.Values[5];
end;

function GetRole(Param: String): String;
begin
  Result := DsnPage.Values[6];
end;

function GetAuth(Param: String): String;
begin
  Result := DsnPage.Values[7];
end;
'@

    Set-Content -Path "setup.iss" -Value $innoScript

    # Create installer for Release configuration
    Write-Step "Creating installer"
    $stepStart = Get-Date

    & $innoCompiler "setup.iss" /Q
    if ($LASTEXITCODE -ne 0) {
        throw "Inno Setup compilation failed"
    }

    $buildResults += @{ Step = "Create Installer"; Duration = (Get-Date) - $stepStart; Status = "Success" }

    # Verify outputs
    Write-Step "Verifying build outputs"
    $stepStart = Get-Date

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

    # Print summary
    $totalDuration = (Get-Date) - $startTime
    Write-Host "`n=== Build Summary ===" -ForegroundColor Green
    Write-Host "Total Duration: $(Format-Duration $totalDuration)" -ForegroundColor Green
    foreach ($result in $buildResults) {
        Write-Host "$($result.Step): $(Format-Duration $result.Duration)" -ForegroundColor Green
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
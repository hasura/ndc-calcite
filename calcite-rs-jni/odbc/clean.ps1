$ErrorActionPreference = 'SilentlyContinue'

# Define paths and patterns
$pathsToClean = @("bin", "obj", "ARM64", "x64", "DDN-ODBC.sln.DotSettings.user", ".vs")
$patternsToClean = @("*.suo", "*.user", "*.cache", "*.dll", "*.pdb", "*.exe", "*.binlog")

Write-Host "`nStarting cleanup..." -ForegroundColor Cyan

function Format-FileSize([int64]$size) {
    if ($size -gt 1GB) { return "{0:N2} GB" -f ($size / 1GB) }
    if ($size -gt 1MB) { return "{0:N2} MB" -f ($size / 1MB) }
    if ($size -gt 1KB) { return "{0:N2} KB" -f ($size / 1KB) }
    return "$size Bytes"
}

$totalSize = 0
$filesRemoved = 0
$dirsRemoved = 0

# Remove dot underscore files first
Get-ChildItem -Path . -Recurse -Force | Where-Object { $_.Name -like "._*" } | ForEach-Object {
    $size = $_.Length
    $totalSize += $size
    Remove-Item $_.FullName -Force
    $filesRemoved++
    Write-Host "Removed $($_.FullName)" -ForegroundColor Yellow
}

# Process directories
foreach ($path in $pathsToClean) {
    Get-ChildItem -Path . -Recurse -Directory -Force | Where-Object { $_.FullName -like "*\$path" } | ForEach-Object {
        if (Test-Path $_.FullName) {
            $size = (Get-ChildItem $_.FullName -Recurse -Force | Measure-Object -Property Length -Sum).Sum
            $totalSize += $size
            $count = (Get-ChildItem $_.FullName -Recurse -Force).Count
            $filesRemoved += $count
            Write-Host "Removing $($_.FullName)" -NoNewline -ForegroundColor Yellow
            Remove-Item $_.FullName -Recurse -Force
            Write-Host " - Done" -ForegroundColor Green
            $dirsRemoved++
        }
    }
}

# Process file patterns
foreach ($pattern in $patternsToClean) {
    Get-ChildItem -Path . -Filter $pattern -Recurse -Force -File | ForEach-Object {
        $size = $_.Length
        $totalSize += $size
        Remove-Item $_.FullName -Force
        $filesRemoved++
    }
}

# Verify cleanup
$remainingItems = Get-ChildItem -Path . -Recurse -Force | Where-Object {
    ($pathsToClean -contains $_.Name) -or
            ($patternsToClean -contains $_.Extension) -or
            ($_.Name -like "._*")
}

Write-Host "`nCleanup Summary:" -ForegroundColor Cyan
Write-Host "Total space cleaned: $(Format-FileSize $totalSize)"
Write-Host "Files removed: $filesRemoved"
Write-Host "Directories removed: $dirsRemoved"

if ($remainingItems) {
    Write-Host "`nWarning: $($remainingItems.Count) items remain:" -ForegroundColor Red
    $remainingItems | ForEach-Object { Write-Host "  $_" -ForegroundColor Yellow }
}

Write-Host "`nCleanup complete!" -ForegroundColor Cyan
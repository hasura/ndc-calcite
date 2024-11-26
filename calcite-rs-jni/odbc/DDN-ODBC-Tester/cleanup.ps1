param ([string]$path)

$cleanPath = $path.TrimEnd('"')
Write-Host "Cleaning path: $cleanPath"
try {
    Get-ChildItem -Path $cleanPath -Filter "._*" -Force -File -Recurse | ForEach-Object {
        Write-Host "Removing $($_.FullName)"
        Remove-Item -Force -LiteralPath $_.FullName
    }
} catch {
    Write-Error "Error cleaning files: $_"
    exit 1
}
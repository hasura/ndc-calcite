$ErrorActionPreference = 'Stop'

function Remove-DotUnderscoreFilesAndDirs {
    param (
        [string]$path
    )

    Write-Host "Processing directory: $path"

    # Get all files and directories starting with ._
    $itemsToRemove = Get-ChildItem -Path $path -Recurse -Force -Filter '._*'

    foreach ($item in $itemsToRemove) {
        try {
            if ($item.PSIsContainer) {
                # Remove directory
                Remove-Item -Path $item.FullName -Recurse -Force
                Write-Host "Removed directory: $($item.FullName)"
            } else {
                # Remove file
                Remove-Item -Path $item.FullName -Force
                Write-Host "Removed file: $($item.FullName)"
            }
        } catch {
            Write-Host "Failed to remove $($item.FullName): $($_.Exception.Message)" -ForegroundColor Red
        }
    }
}

# Set the root path to the current working directory
$rootPath = Get-Location

# Call the function
Remove-DotUnderscoreFilesAndDirs -path $rootPath

Write-Host "Cleanup completed."

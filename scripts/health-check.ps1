param(
    [Parameter(Mandatory=$false)]
    [string]$url = "http://localhost:8080"
) 

# error preference
$ErrorActionPreference = "Stop"

$url = $url + "/api/v1/health"

$response = Invoke-WebRequest -Uri $url -Method Get

if ($response.StatusCode -eq 200) {
    Write-Host "Health check passed"
    exit 0
} else {
    Write-Host "Health check failed"
    exit 1
}

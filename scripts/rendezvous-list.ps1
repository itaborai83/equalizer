param(
    [Parameter(Mandatory=$false)]
    [string]$url = "http://localhost:8080"
) 

$ErrorActionPreference = "Stop"

$url = $url + "/api/v1/rendezvous"

$headers = @{}

try {
    $response = Invoke-RestMethod -Uri $url -Method Get -Headers $headers
    Write-Output $response | ConvertTo-Json -Depth 10
} catch {
    Write-Error "Failed to create rendezvous: $_"
    exit 1
}

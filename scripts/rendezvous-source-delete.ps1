param(
    [Parameter(Mandatory=$false)]
    [string]$Url = "http://localhost:8080",

    [Parameter(Mandatory=$true)]
    [string]$Name,

    [Parameter(Mandatory=$true)]
    [string]$AuthToken
)

$ErrorActionPreference = "Stop"

. ./scripts/lib/utils.ps1

if (-not $Name) {
    Write-Error "Name is required"
    exit 1
}

if (-not $AuthToken) {
    Write-Error "AuthToken is required"
    exit 1
}

$headers = @{
    "Authorization" = "Bearer $AuthToken"
}

try {
    $url = $url + "/api/v1/rendezvous/$Name/source"
    $response = Invoke-RestMethod -Uri $url -Method Delete -Headers $headers -ContentType "application/json"
    Write-Output $response | ConvertTo-Json -Depth 10
} catch {
    Write-Error "Failed to create rendezvous: $_"
    exit 1
}
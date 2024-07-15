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

$body = @{
    "name" = $Name
}

$payload = $body | ConvertTo-Json

try {
    $url = $url + "/api/v1/rendezvous/$Name/equalize"
    $response = Invoke-RestMethod -Uri $url -Method Post -Headers $headers -Body $payload -ContentType "application/json"
} catch {
    Write-Error "Failed to equalize rendezvous data: $_"
    exit 1
}

Write-Output $response | ConvertTo-Json -Depth 10




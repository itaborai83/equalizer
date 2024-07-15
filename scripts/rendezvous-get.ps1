param(
    [Parameter(Mandatory=$false)]
    [string]$url = "http://localhost:8080",

    [Parameter(Mandatory=$false)]
    [string]$Name,

    [Parameter(Mandatory=$true)]
    [string]$AuthToken

) 

$ErrorActionPreference = "Stop"

if (-not $Name) {
    Write-Error "Name is required"
    exit 1
}

if (-not $AuthToken) {
    Write-Error "AuthToken is required"
    exit 1
}

$url = $url + "/api/v1/rendezvous/$Name"

$headers = @{
    "Authorization" = "Bearer $AuthToken"
}


try {
    $response = Invoke-RestMethod -Uri $url -Method Get -Headers $headers
    Write-Output $response | ConvertTo-Json -Depth 10
} catch {
    Write-Error "Failed to retrieve rendezvous: $_"
    exit 1
}

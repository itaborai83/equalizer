param(
    [Parameter(Mandatory=$false)]
    [string]$Url = "http://localhost:8080",

    [Parameter(Mandatory=$true)]
    [string]$Name,

    [Parameter(Mandatory=$true)]
    [string]$DataFile,

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

if (-not $DataFile) {
    Write-Error "Source data file is required"
    exit 1
}

if (-not (Test-Path $DataFile)) {
    Write-Error "Source data file does not exist"
    exit 1
}
$data = Get-Content $DataFile -Raw | ConvertFrom-Json

$headers = @{
    "Authorization" = "Bearer $AuthToken"
}

$requestBody = $data | ConvertTo-Json -Depth 10

try {
    $url = $url + "/api/v1/rendezvous/$Name/source"
    $response = Invoke-RestMethod -Uri $url -Method Put -Headers $headers -Body $requestBody -ContentType "application/json"
    Write-Output $response | ConvertTo-Json -Depth 10
} catch {
    Write-Error "Failed to create rendezvous: $_"
    exit 1
}




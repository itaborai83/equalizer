param(
    [Parameter(Mandatory=$false)]
    [string]$Url = "http://localhost:8080",

    [Parameter(Mandatory=$true)]
    [string]$Name,

    [Parameter(Mandatory=$true)]
    [string]$Data,

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

if (-not $Data) {
    Write-Error "Data is required"
    exit 1
}

# valid data are: insert, update, delete, equalized
if ($Data -ne "insert" -and $Data -ne "update" -and $Data -ne "delete" -and $Data -ne "equalized") {
    Write-Error "Data must be one of the following: insert, update, delete, equalized"
    exit 1
}

$headers = @{
    "Authorization" = "Bearer $AuthToken"
}

try {
    $url = $url + "/api/v1/rendezvous/$Name/result/$Data"
    $response = Invoke-RestMethod -Uri $url -Method Get -Headers $headers -ContentType "application/json"
    Write-Output $response
} catch {
    Write-Error "Failed to retrieve data: $_"
    exit 1
}
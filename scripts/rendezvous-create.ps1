param(
    [Parameter(Mandatory=$false)]
    [string]$Url = "http://localhost:8080",

    [Parameter(Mandatory=$false)]
    [string]$Name = "Rendezvous",

    [Parameter(Mandatory=$true)]
    [string]$SourceSpecFile,

    [Parameter(Mandatory=$true)]
    [string]$TargetSpecFile,

    [Parameter(Mandatory=$true)]
    [string]$AuthToken
)

$ErrorActionPreference = "Stop"

. ./scripts/lib/utils.ps1

if (-not (Test-Path $SourceSpecFile)) {
    Write-Error "Source spec file does not exist"
    exit 1
}
$sourceSpec = Get-Content $SourceSpecFile -Raw | ConvertFrom-Json

# does the target spec file exist?
if (-not (Test-Path $TargetSpecFile)) {
    Write-Error "Target spec file does not exist"
    exit 1
}
# read json file and convert to object
$targetSpec = Get-Content $TargetSpecFile -Raw | ConvertFrom-Json

$headers = @{
}

# create the rendezvous
$rendezvous = @{
    "source_spec" = $sourceSpec
    "target_spec" = $targetSpec
    "auth_token" = $AuthToken
}
$requestBody = $rendezvous | ConvertTo-Json -Depth 10


try {
    $url = $url + "/api/v1/rendezvous/$Name"
    $response = Invoke-RestMethod -Uri $url -Method Put -Headers $headers -Body $requestBody -ContentType "application/json"
    Write-Output $response | ConvertTo-Json -Depth 10
} catch {
    Write-Error "Failed to create rendezvous: $_"
    exit 1
}




param(
    [Parameter(Mandatory=$false)]
    [string]$url = "http://localhost:8080"
) 

# error preference
$ErrorActionPreference = "Stop"

$url = $url + "/api/v1/rendezvous"

$response = Invoke-WebRequest -Uri $url -Method Get
$json = $response.Content | ConvertFrom-Json

if ($response.StatusCode -eq 200) {
    Write-Host $json
} else {
    Write-Error $json
}

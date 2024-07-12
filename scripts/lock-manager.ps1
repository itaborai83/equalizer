param(
    [Parameter(Mandatory=$true)]
    [string]$BasePath,
    
    [Parameter(Mandatory=$true)]
    [string]$LockName,

    [Parameter(Mandatory=$false)]
    [bool]$Unlock = $false,

    [Parameter(Mandatory=$false)]
    [int]$Timeout = 60,

    [Parameter(Mandatory=$false)]
    [bool]$Wait = $false
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $BasePath)) {
    Write-Host "Base path does not exist: $BasePath"
    exit
}

if (-not $LockName) {
    Write-Host "Lock name is required"
    exit
}

# replace backslashes with forward slashes
$BasePath = $BasePath -replace '\\', '/'

if ($Unlock) {
    go run ./cmd/lock/main.go -dir $BasePath -lock $LockName -unlock
    exit
} 

if ($Wait) {
    go run ./cmd/lock/main.go -dir $BasePath -lock $LockName -wait -timeout $Timeout
    exit
}

go run ./cmd/lock/main.go -dir $BasePath -lock $LockName

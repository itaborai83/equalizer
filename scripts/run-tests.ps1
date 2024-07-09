Param(
    [Parameter(Mandatory=$false)]
    [bool]$VerboseTests = $false
)

if ($verbose) {
    $env:GO111MODULE = "on"
    go test -v ./...
} else {
    go test ./...
}

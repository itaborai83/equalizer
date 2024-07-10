Param(
    [Parameter(Mandatory=$false)]
    [bool]$VerboseTests = $false
)

if ($verbose) {
    $env:GO111MODULE = "on"
    go test -count=1 -v ./...
} else {
    go test -count=1 ./...
}

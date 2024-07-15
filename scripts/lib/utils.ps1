$ErrorActionPreference = "Stop"

function Remove-PSProperties {
    param (
        [Parameter(ValueFromPipeline = $true)]
        [psobject]$InputObject
    )

    process {
        $result = @{}

        foreach ($property in $InputObject.PSObject.Properties) {
            if ($property.Name -notmatch "^PS") {
                $result[$property.Name] = $property.Value
            }
        }

        return $result
    }
}

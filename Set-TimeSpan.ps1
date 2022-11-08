Function Set-TimeSpan {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true, Position=0)][TimeSpan]$Span
    )
    Write-Output $Span
}

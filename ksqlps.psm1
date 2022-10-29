$global:DefaultKsqlUrl = 'http://localhost:8088'
Function Set-KsqlUrl {
    [CmdletBinding()]
    Param(
        [Parameter(Mandatory=$True)][String]$Url
    )
    $global:DefaultKsqlUrl = $Url;
}

Function Get-KsqlUrl {
    [CmdletBinding()]
    Param(
        [Parameter()][String]$Path
    )
    $Url = $global:DefaultKsqlUrl
    if(-not [String]::IsNullOrWhiteSpace($Path)){
        $Url = "$Url/$Path"
    }
    return $Url
}

Function Invoke-KsqlCommand {
    [CmdletBinding()]
    Param(
        [Parameter(Mandatory=$True)][String]$Command
    )
    Try {
		$Body = $BodyParameters | ConvertTo-JSON -Depth 6
		Write-Verbose $Body
		if($PSCmdlet.ShouldProcess($Uri)){
			Invoke-RestMethod -Uri $Uri -Method $Method -Headers @{'Authorization'=$(Convert-CredentialToAuthHeader -Credential ($Connection.Credential))} -Body $Body
		}
	}
	Catch {
		return $null
	}
}

Function Add-KsqlStreamValue {
    [CmdletBinding()]
    Param(
        [Parameter(Mandatory=$True)][String]$StreamName,
        [Parameter(ValueFromPipeline, ParameterSetName="Value")][Hashtable[]]$InputObject,
        [Parameter(ParameterSetName="Value")][String]$KeyPropertyName = "Key",
        [Parameter(ParameterSetName="NoValue")][String]$KeyValue
    )

    $InputObject.ForEach({
        $Object = $_

        $Columns = @()
        $Object.GetEnumerator().ForEach({
            $Columns += $_.Name
        })
        $Columns = $Columns -join ','
        Write-Verbose "Columns: $Columns"

        $Values = @()
        $Object.GetEnumerator().ForEach({
            $Values += "'$($_.Value)'"
        })
        $Values = $Values -join ','
        Write-Verbose "Values: $Values"
        
        $Body = @{}
        $Body.ksql = "INSERT INTO $StreamName ($Columns) VALUES ($Values);"
        $Body = $Body | ConvertTo-Json -EscapeHandling Default

        $Url = Get-KsqlUrl ksql
        
        Write-Verbose "POST $Url"
        Write-Verbose "$Body"
        Invoke-RestMethod -Uri $Url -Method POST -Body $Body
    })
}

Function New-KsqlStream {
    [CmdletBinding()]
    Param(
        [Parameter(Mandatory=$True)][String]$StreamName,
        [Parameter(Mandatory=$True)][HashMap[]]$ColumnData,
        [Parameter(Mandatory=$True)][String]$KeyColumnName
    )
}

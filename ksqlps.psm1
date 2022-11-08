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

Function Invoke-KsqlQuery {
    [CmdletBinding(SupportsShouldProcess=$True)]
    Param(
        [Parameter(Mandatory=$True)][String]$Query,
        [Parameter()][Hashtable]$StreamProperties = @{}
    )

    $Body = @{}
    $Body.ksql = $Query
    $Body.streamsProperties = $StreamProperties
    $Body = $Body | ConvertTo-Json -Depth 5
    $Body = [regex]::replace( # See https://stackoverflow.com/questions/47779157/convertto-json-and-convertfrom-json-with-special-characters
        $Body, 
        '(?<=(?:^|[^\\])(?:\\\\)*)\\u(00(?:26|27|3c|3e))', 
        { param($match) [char] [int] ('0x' + $match.Groups[1].Value) },
        'IgnoreCase'
    )

    $Url = Get-KsqlUrl query
    
    Write-Verbose "POST $Url"
    Write-Verbose "$Body"
    if($PSCmdlet.ShouldProcess($Url)){
        Invoke-RestMethod -Uri $Url -Method POST -Body $Body -TransferEncoding chunked -ContentType 'application/json'
    }
}

Function Invoke-KsqlCommand {
    [CmdletBinding(SupportsShouldProcess=$True)]
    Param(
        [Parameter(Mandatory=$True)][String]$Command
    )

    $Body = @{}
    $Body.ksql = $Command
    $Body = $Body | ConvertTo-Json -Depth 5
    $Body = [regex]::replace( # See https://stackoverflow.com/questions/47779157/convertto-json-and-convertfrom-json-with-special-characters
        $Body, 
        '(?<=(?:^|[^\\])(?:\\\\)*)\\u(00(?:26|27|3c|3e))', 
        { param($match) [char] [int] ('0x' + $match.Groups[1].Value) },
        'IgnoreCase'
    )

    $Url = Get-KsqlUrl ksql
    
    Write-Verbose "POST $Url"
    Write-Verbose "$Body"
    if($PSCmdlet.ShouldProcess($Url)){
        Invoke-RestMethod -Uri $Url -Method POST -Body $Body -Headers @{'Accept'='application/vnd.ksql.v1+json'}
    }
}

Function Add-KsqlStreamValue {
    [CmdletBinding(SupportsShouldProcess=$True)]
    Param(
        [Parameter(Mandatory=$True)][String]$StreamName,
        [Parameter(ValueFromPipeline, ParameterSetName="Value")][Hashtable[]]$InputObject,
        [Parameter(ParameterSetName="Value")][String]$KeyPropertyName = "Key",
        [Parameter(ParameterSetName="NoValue")][String]$KeyValue
    )

    $InputObject.ForEach({
        $Columns = @()
        $_.GetEnumerator().ForEach({
            $Columns += $_.Name
        })
        $Columns = $Columns -join ','

        $Values = @()
        $_.GetEnumerator().ForEach({
            $Values += "'$($_.Value)'"
        })
        $Values = $Values -join ','
        
        $Command = "INSERT INTO $StreamName ($Columns) VALUES ($Values);"
        Invoke-KsqlCommand $Command -Verbose:([bool]$PSBoundParameters['Verbose'].IsPresent) -WhatIf:([bool]$PSBoundParameters['WhatIf'].IsPresent)
    })
}

Function New-KsqlStream {
    [CmdletBinding(SupportsShouldProcess=$True)]
    Param(
        [Parameter(Mandatory=$True)][String]$StreamName,
        [Parameter(Mandatory=$True)][String[]]$ColumnData, # ColumnName:ColumnType e.g. Name:string,Size:int
        [Parameter(Mandatory=$True)][String]$KeyColumnName,
        [Parameter()][String]$TopicName,
        [Parameter()][Switch]$Replace,
        [Parameter()][Switch]$IfNotExists,
        [Parameter()][Switch]$Source,
        [Parameter()][Int]$PartitionCount = 1,
        [Parameter()][String]$Format='JSON'
    )
    $Script:FoundKeyColumn = $False
    $Columns = @()
    $ColumnData.ForEach({
        if($_ -notmatch "^(?<column>\w+?):(?<type>(boolean|string|bytes|int|bigint|double|decimal|time|date|timestamp)$)") {
            Throw "Format of column descriptor must be <column name>:<scalar type name>"
        }
        $Column = "$($Matches.column) $($Matches.type)"
        if($KeyColumnName -eq $Matches.column) {
            $Script:FoundKeyColumn = $True
            $Column += " KEY"
        }
        $Columns += $Column
    })
    if(-not $Script:FoundKeyColumn) {
        Throw "KeyColumnName not found in column descriptors"
    }
    if([String]::IsNullOrWhiteSpace($TopicName)){
        $TopicName = $StreamName
    }
    $Command = "CREATE $(if($Replace) {"OR REPLACE "})$(if($Source){"SOURCE "})STREAM $(if($IfNotExists){"IF NOT EXISTS "}) $StreamName ($($Columns -join ',')) WITH (KAFKA_TOPIC='$TopicName',PARTITIONS=$PartitionCount,FORMAT='$Format');"
    Invoke-KsqlCommand $Command -Verbose:([bool]$PSBoundParameters['Verbose'].IsPresent) -WhatIf:([bool]$PSBoundParameters['WhatIf'].IsPresent)
}

Function Remove-KsqlStream {
    [CmdletBinding(SupportsShouldProcess=$True)]
    Param(
        [Parameter(Mandatory=$True)][String]$StreamName,
        [Parameter()][Switch]$DeleteTopic
    )

    $Command = "DROP STREAM $StreamName"
    if($DeleteTopic){
        $Command += " DELETE TOPIC"
    }
    $Command += ';'
    Invoke-KsqlCommand $Command -Verbose:([bool]$PSBoundParameters['Verbose'].IsPresent) -WhatIf:([bool]$PSBoundParameters['WhatIf'].IsPresent)
}

Function New-KsqlTable {
    [CmdletBinding(SupportsShouldProcess=$True)]
    Param(
        [Parameter(Mandatory=$True)][String]$TableName,
        [Parameter(Mandatory=$True)][String[]]$ColumnData, # ColumnName:ColumnType e.g. Name:string,Size:int
        [Parameter(Mandatory=$True)][String]$KeyColumnName,
        [Parameter()][String]$TopicName,
        [Parameter()][Switch]$Replace,
        [Parameter()][Switch]$IfNotExists,
        [Parameter()][Switch]$Source,
        [Parameter()][Int]$PartitionCount = 1,
        [Parameter()][String]$Format='JSON'
    )
    $Script:FoundKeyColumn = $False
    $Columns = @()
    $ColumnData.ForEach({
        if($_ -notmatch "^(?<column>\w+?):(?<type>(boolean|string|bytes|int|bigint|double|decimal|time|date|timestamp)$)") {
            Throw "Format of column descriptor must be <column name>:<scalar type name>"
        }
        $Column = "$($Matches.column) $($Matches.type)"
        if($KeyColumnName -eq $Matches.column) {
            $Script:FoundKeyColumn = $True
            $Column += " PRIMARY KEY"
        }
        $Columns += $Column
    })
    if(-not $Script:FoundKeyColumn) {
        Throw "KeyColumnName not found in column descriptors"
    }
    if([String]::IsNullOrWhiteSpace($TopicName)){
        $TopicName = $TableName
    }
    $Command = "CREATE $(if($Replace) {"OR REPLACE "})$(if($Source){"SOURCE "})TABLE $(if($IfNotExists){"IF NOT EXISTS "})$TableName ($($Columns -join ',')) WITH (KAFKA_TOPIC='$TopicName',PARTITIONS=$PartitionCount,FORMAT='$Format');"
    Invoke-KsqlCommand $Command -Verbose:([bool]$PSBoundParameters['Verbose'].IsPresent) -WhatIf:([bool]$PSBoundParameters['WhatIf'].IsPresent)
}

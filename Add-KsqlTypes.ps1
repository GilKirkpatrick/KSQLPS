Add-Type @"
using System;
namespace semperis.research.ksqlps
{
    public enum KsqlColumnType {
        @bool,
        @int,
        @bigint,
        @double,
        @string
    }
}
"@

Add-Type @"
using System;
namespace semperis.research.ksqlps
{
    public struct KsqlFieldDefinition {
        KsqlColumnType fieldType;
        string fieldName;
    }
}
"@

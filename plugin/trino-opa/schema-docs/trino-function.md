# Trino function

This object contains information about a Trino function or procedure.

## Fields

|Field name|Always set|Type|Description|
|:-:|:-:|:-:|-|
|`catalogName`|❌|`string`|Catalog this procedure belongs to|
|`schemaName`|❌<br/>If `catalogName` is set, `schemaName` will also be set|`string`|Schema this procedure belongs to|
|`functionName`|✅|`string`|Name of the function or procedure|

## Example - function does not belong to a schema

```json5
{
    "functionName": "some_function_or_procedure"
}
```

## Example - function belongs to a schema

```json5
{
    "catalogName": "some_catalog",
    "schemaName": "some_schema",
    "functionName": "some_function_or_procedure"
}
```

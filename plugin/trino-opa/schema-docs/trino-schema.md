# Trino schema object

This object contains information about a Trino schema, as well as - optionally - its properties.

Properties are only included when the properties of a schema are being set, at the moment that is only on creation of the schema.

## Fields

|Field name|Always set|Type|Description|
|:-:|:-:|:-:|-|
|`catalogName`|✅|`string`|Name of the catalog this schema belongs to|
|`schemaName`|✅|`string`|Name of the schema|
|`properties`|❌|`Map[string, any]`|Properties the schema is being created with - only set when creating a schema|

## Example - no properties

```json5
{
    "catalogName": "some_catalog",
    "schemaName": "some_schema"
}
```

## Example - with properties

```json5
{
    "catalogName": "some_catalog",
    "schemaName": "some_schema",
    "properties": {
        "some_property": "some_property_value"}
    }
}
```

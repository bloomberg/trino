# Trino table

This object contains information about a Trino table **or view**, as well as - optionally - its properties and/or column(s).

Properties are only included for operations that create or modify table/view properties.

Columns are only included for operations that access, alter or create column(s) on the table/view.

> At present, there is no operation that involves _both_ columns and properties

## Fields

|Field name|Always set|Type|Description|
|:-:|:-:|:-:|-|
|`catalogName`|✅|`string`|Name of the catalog this table belongs to|
|`schemaName`|✅|`string`|Name of the schema this table belongs to|
|`tableName`|✅|`string`|Name of the table|
|`columns`|❌|`Array[string]`|Column(s) involved in the operation|
|`properties`|❌|`Map[string, any]`|Table/View properties being created or modified|

## Example - no properties

```json5
{
    "catalogName": "some_catalog",
    "schemaName": "some_schema",
    "tableName": "some_table"
}
```

## Example - with properties

```json5
{
    "catalogName": "some_catalog",
    "schemaName": "some_schema",
    "tableName": "some_table",
    "properties": {
        "some_property": "some_property_value"}
    }
}
```

## Example - with columns

```json5
{
    "catalogName": "some_catalog",
    "schemaName": "some_schema",
    "tableName": "some_table",
    "columns": ["column_one", "column_two"]
}
```

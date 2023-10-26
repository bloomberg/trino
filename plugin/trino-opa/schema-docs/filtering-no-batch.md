# Filtering operations: non-batch mode

For the non-batch mode, the plugin will send a request to OPA for **each** resource being filtered.
For every element being filtered, an OPA request will be sent containing the appropriate item under the `resource` field
in the `action` block.

Only those items for which OPA responds with `true` will be included in the filtered set.

These requests look identical to any other operation, in that the `action` block contains a `resource` and an `operation` field.

|Operation|`resource` field content|
|-|-|
|`FilterFunctions`|`function` object ([docs](./trino-function.md))|
|`FilterViewQueryOwnedBy`|`user` object ([docs](./trino-identity.md))|
|`FilterCatalogs`|`catalog` object ([docs](./trino-catalog.md))|
|`FilterSchemas`|`schema` object ([docs](./trino-schema.md)) with no properties|
|`FilterTables`|`table` object ([docs](./trino-table.md)) with no properties or columns|
|`FilterColumns`|`table` object ([docs](./trino-table.md)) with a single column|

## Expected response from OPA

For the non-batch mode, the response expected from OPA is identical to any
other request: a simple boolean value denoting whether the specific `resource` in the request
should be included in the filtered result set.

## `FilterFunctions` example

```json5
{
    // context: {...},
    "action": {
        "operation": "FilterFunctions",
        "resource": {
            "function": {
                "catalogName": "some_catalog",
                "schemaName": "some_schema",
                "functionName": "some_function"
            }
        }
    }
}
```

## `FilterViewQueryOwnedBy` example

```json5
{
    // context: {...},
    "action": {
        "operation": "FilterViewQueryOwnedBy",
        "resource": {
            "user": {
                "user": "some_username",
                "groups": [],
                "extraCredentials": {}
            }
        }
    }
}
```

## `FilterCatalogs` example

```json5
{
    // context: {...},
    "action": {
        "operation": "FilterCatalogs",
        "resource": {
            "catalog": {
                "name": "catalog_one"
            }
        }
    }
}
```

## `FilterSchemas` example

```json5
{
    // context: {...},
    "action": {
        "operation": "FilterSchemas",
        "resource": {
            "schema": {
                "catalogName": "some_catalog",
                "schemaName": "schema_one"
            }
        }
    }
}
```

## `FilterTables` example

```json5
{
    // context: {...},
    "action": {
        "operation": "FilterTables",
        "resource": {
            "table": {
                "catalogName": "some_catalog",
                "schemaName": "some_schema",
                "tableName": "table_one"
            }
        }
    }
}
```

## `FilterColumns` example

Suppose we are filtering two columns under `some_catalog.some_schema.some_table`: `column_one` and `column_two`.

OPA will send a request for each column, each containing a single column.

```json5
{
    // context: {...},
    "action": {
        "operation": "FilterColumns",
        "resource": {
            "table": {
                "catalogName": "some_catalog",
                "schemaName": "some_schema",
                "tableName": "some_table",
                "columns": ["column_one"]
            }
        }
    }
}
```

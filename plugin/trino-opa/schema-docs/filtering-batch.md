# Filtering operations: batch mode

For the batch mode, the plugin will reduce round trips to OPA by sending all resources being filtered
in a single request. I.e. the `resource` field is not used, and instead a list of resources is included under
`filterResources`.

The OPA policy is expected to respond with the the indices of the elements in the request's `filterResources` field
that should be included in the filtered result set.

|Operation|`filterResources` field content|
|-|-|
|`FilterFunctions`|`Array[function]` ([docs](./trino-function.md))|
|`FilterViewQueryOwnedBy`|`Array[user]` ([docs](./trino-identity.md))|
|`FilterCatalogs`|`Array[catalog]` ([docs](./trino-catalog.md))|
|`FilterSchemas`|`Array[schema]` ([docs](./trino-schema.md)) with no properties|
|`FilterTables`|`Array[table]` ([docs](./trino-table.md)) with no properties or columns|
|`FilterColumns`|`Array[table]` ([docs](./trino-table.md)) - special case, see below|

A batch request therefore has the following format:

```json5
{
    // context: {...},
    "action": {
        "operation": "SomeOperation",  // e.g. FilterCatalogs
        "filterResources": [
            // list of resources, e.g. {"catalog": {"name": "catalog_one"}}
        ]
    }
}
```

## Expected response from OPA

When batch mode is enabled, the response format expected from OPA is different (only for these operations).

Instead of a single boolean value, the OPA response needs to convey the (potentially empty) set of elements
that the user should be entitled to discover.

Responses are expected to be in the format of an `Array[int]`, where each integer is the index of a resource
from the request's `filterResources` array that the user should be allowed to discover.

### Expected request & response

**Request:**
```json5
{
    // context: {...},
    "action": {
        "operation": "FilterCatalogs",
        "filterResources": [
            {"catalog": {"name": "catalog_one"}},
            {"catalog": {"name": "catalog_two"}},
            {"catalog": {"name": "catalog_three"}}
        ]
    }
}
```

**Sample response:**

```json5
[0, 2]
```

**Final result:**

The user would only be allowed to see `catalog_one` and `catalog_three`.

### Corner cases

- OPA responds with an empty list: nothing will be visible to the user
- OPA responds with out of bounds indices in the list: the query will be failed
- OPA responds with any other error or invalid JSON response: the query will be failed


## `FilterFunctions` example

```json5
{
    // context: {...},
    "action": {
        "operation": "FilterFunctions",
        "filterResources": [
            {
                "function": {
                    "catalogName": "some_catalog",
                    "schemaName": "some_schema",
                    "functionName": "some_function"
                }
            },
            {
                "function": {
                    "catalogName": "some_catalog",
                    "schemaName": "some_schema",
                    "functionName": "another_function"
                }
            }
        ]
    }
}
```

## `FilterViewQueryOwnedBy` example

```json5
{
    // context: {...},
    "action": {
        "operation": "FilterViewQueryOwnedBy",
        "filterResources": [
            {
                "user": {
                    "user": "some_username",
                    "groups": [],
                    "extraCredentials": {}
                }
            },
            {
                "user": {
                    "user": "another_username",
                    "groups": [],
                    "extraCredentials": {}
                }
            }
        ]
    }
}
```

## `FilterCatalogs` example

```json5
{
    // context: {...},
    "action": {
        "operation": "FilterCatalogs",
        "filterResources": [
            {
                "catalog": {
                    "name": "catalog_one"
                }
            },
            {
                "catalog": {
                    "name": "catalog_two"
                }
            }
        ]
    }
}
```

## `FilterSchemas` example

```json5
{
    // context: {...},
    "action": {
        "operation": "FilterSchemas",
        "filterResources": [
            {
                "schema": {
                    "catalogName": "some_catalog",
                    "schemaName": "schema_one"
                }
            },
            {
                "schema": {
                    "catalogName": "some_catalog",
                    "schemaName": "schema_two"
                }
            }
        ]
    }
}
```

## `FilterTables` example

```json5
{
    // context: {...},
    "action": {
        "operation": "FilterTables",
        "filterResources": [
            {
                "table": {
                    "catalogName": "some_catalog",
                    "schemaName": "some_schema",
                    "tableName": "table_one"
                }
            },
            {
                "table": {
                    "catalogName": "some_catalog",
                    "schemaName": "some_schema",
                    "tableName": "table_two"
                }
            }
        ]
    }
}
```

## `FilterColumns` - a corner case

`FilterColumns` is a corner case, as the [table object](./trino-table.md) already contains a list of columns.

Therefore, unlike for any other request, a `FilterColumns` batch request will:
- Contain a _single_ table object under the `filterResources` array
- Said table will contain a _list_ of all columns being filtered for that table

> If more than one table is involved in the `FilterColumns` operation, one request will be sent per table
>
> Each request will contain all the columns specific to that table

The OPA response format is the same as for other batch filter operations, but the indices refer to
the indices of the columns within the single `table` object in `filterResources`.

### `FilterColumns` example

Suppose we are filtering columns for table `some_catalog.some_schema.some_table`. This table has
3 columns: `column_one`, `column_two` and `column_three`.

**Request:**

```json5
{
    // context: {...},
    "action": {
        "operation": "FilterColumns",
        "filterResources": [
            {
                "table": {
                    "catalogName": "some_catalog",
                    "schemaName": "some_schema",
                    "tableName": "some_table",
                    "columns": ["column_one", "column_two", "column_three"]
                }
            }
        ]
    }
}
```

> Notice how there is a _single_ object under `filterResources`

**Sample response:**

```json5
[0, 2]
```

> 0 and 2 refer to the indices of the `columns` field in the table

**Final result:**

`column_one` and `column_three` will be visible for the user

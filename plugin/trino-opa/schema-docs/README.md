# trino-opa schema

This document contains the specification for the requests sent by the authorizer for each operation.

## Overall format of a request

An authorization request has the following overall format, each section will be covered in-depth in its own section:

```json5
{
    "context": {
        "identity": {
            "user": "username",
            "groups": [],
            "extraCredentials": {}
        },
        "softwareStack": {
            "trinoVersion": "431"
        }
    },
    "action": {
        "operation": "<operation name>",
        // All the following fields are optional
        "resource": {},
        "filterResources": [],
        "targetResource": {},
        "grantee": {},
        "grantor": {}
    }
}
```

There are 2 top level fields on a request, both of these are _always_ present:

- `context`: common across all request types, contains contextual information such as _who_ is performing the operation
- `action`: denotes _what_ operation is being performed, as well as the resource(s) involved on said operation (if any)

## The `context` object

This is common to all requests. It is always present and always contains the same fields, regardless of the operation being performed.

- `identity`: identity of the user performing the operation - [full definition](./trino-identity.md)
- `softwareStack`: information about Trino (TODO - this is not done yet) - [docs](#contextsoftwarestack)

`context` object example:

```json5
{
    "identity": {
        "user": "some-user",
        "groups": ["group-1", "group-2"],
        "extraCredentials": {}
    },
    "softwareStack": {
        "trinoVersion": "431"
    }
}

```
### `context.softwareStack`

> This is not yet implemented

This object contains information about the software stack. Extra fields may be added
in the future to convey additional information about the software stack.

Currently, this object contains the following field:
- `trinoVersion`: Trino version

```json5
{
    "trinoVersion": "431"
}
```

## The `action` object

The `action` object contains information about the operation being performed,
as well as the resources involved.

> Many operations share the same request format since they operate on the same resource type - e.g. the overall format of
> an authorization request for inserting data into a table is the same as that for deleting rows from a table.
>
> As such, this document groups requests based on the overall format of the request.

The following fields may be set on the `action` object:

|Field name|Required|Description|
|:-:|:-:|-|
|`operation`|✅|Action being performed, it reflects the operation name as defined on the SPI|
|`resource`|❌|Resource being operated on, e.g. what table is being dropped|
|`targetResource`|❌ <br/>If set, `resource` must also be set|When a `resource` is being modified, this field contains the new definition of the `resource` - e.g. new table name if renaming a table|
|`filterResources`|❌ <br/>This field _cannot_ be set at the same time as `resource`|Only used for filtering in batch mode, list of `resource` items|
|`grantee`|❌|Identity targeting granted / revoked|
|`grantor`|❌|Identity of the user granting or revoking a grant|


### Simple operations

Operations:

- `ExecuteQuery`
- `ReadSystemInformation`
- `WriteSystemInformation`
- `ShowCurrentRoles`
- `ShowRoleGrants`
- `ShowRoles`

These operations do not perform actions on a specific object/grant/user, they are fully
self-defined.

As such, only the `operation` field in the `action` object will be populated:


The `action` object for a simple operation would look as follows:

```json5
{
    "operation": "SomeOperation"  // e.g. "ExecuteQuery"
}
```

### `user` operations

Operations:
- `ViewQueryOwnedBy`
- `KillQueryOwnedBy`

These operations perform actions on a `user` object. A `user` object is defined by the
[trino identity](./trino-identity.md) object, which is identical in format to the one in the `context` of the request.

#### Example:

```json5
{
    "operation": "SomeOperation",  // e.g. "ViewQueryOwnedBy"
    "resource": {
        "user": {
            "user": "some-user",
            "groups": ["some-group"],
            "extraCredentials": {}
        }
    }
}
```

### User impersonation

Operations:
- `ImpersonateUser`

Attempting to impersonate a user will send a request containing a `user` resource with _only_ the username.
This means the `resource` field looks like the [trino identity](./trino-identity.md) object used elsewhere,
but without any field other than `user` (username).

#### Example:

```json5
{
    "operation": "ImpersonateUser",
    "resource": {
        "user": {
            "user": "target_username"
        }
    }
}
```

### `systemSessionProperty` operations

Operations:
- `SetSystemSessionProperty`

This operation performs an action on a `systemSessionProperty` object, whose only property is `name`

The following fields are set:

|Field name|Always set|Description|
|:-:|:-:|-|
|`operation`|✅|Operation being performed (`SetSystemSessionProperty`)|
|`resource.systemSessionProperty.name`|✅|Name of the property being set|

#### Example:

```json5
{
    "operation": "SetSystemSessionProperty",
    "resource": {
        "systemSessionProperty": {
            "name": "some_property"
        }
    }
}
```

### `catalog` operations

Operations:
- `AccessCatalog`
- `CreateCatalog`
- `DropCatalog`
- `ShowSchemas`

These operations perform actions on a `catalog` object, whose only property is `name` (see [catalog object](./trino-catalog.md))

The following fields are set:

|Field name|Always set|Description|
|:-:|:-:|-|
|`operation`|✅|Operation being performed|
|`resource.catalog.name`|✅|Name of the catalog being operated on|

#### Example:

```json5
{
    "operation": "SomeOperation",  // e.g. AccessCatalog
    "resource": {
        "catalog": {
            "name": "some_catalog"
        }
    }
}
```

### `schema` operations

These operations perform actions on a [`schema` object](./trino-schema.md).
Only one of these operations will populate the `properties` field on the schema.

|Operation|Has properties|
|-|:-:|
|`CreateSchema`|✅|
|`DropSchema`|❌|
|`ShowCreateSchema`|❌|
|`ShowTables`|❌|
|`ShowFunctions`|❌|

Fields:

|Field name|Always set|Description|
|:-:|:-:|-|
|`operation`|✅|Operation being performed|
|`resource.schema`|✅|Schema information ([definition here](./trino-schema.md))|

#### Example - schema creation (with properties):

```json5
{
    "operation": "CreateSchema",
    "resource": {
        "schema": {
            "catalogName": "some_catalog",
            "schemaName": "some_schema",
            "properties": {
                "schema_property": "schema_property_value"
            }
        }
    }
}
```

#### Example - other operations without properties:

```json5
{
    "operation": "SomeOperation",  // e.g. DropSchema
    "resource": {
        "schema": {
            "catalogName": "some_catalog",
            "schemaName": "some_schema"
        }
    }
}
```

### `table` operations - operations acting on tables _or_ views

These operations perform actions on a Trino table or a Trino view. As far as the OPA plugin
is concerned, these are both defined by the [`table` object](./trino-table.md).

As explained in the `table` object documentation, the `table` object can optionally contain columns and properties.
These fields are only set for operations that involve columns and properties respectively, as outlined below.

|Operation|Properties field set?|Columns field set?|
|-|:-:|:-:|
|`ShowCreateTable`|❌|❌|
|`CreateTable`|✅|❌|
|`DropTable`|❌|❌|
|`SetTableProperties`|✅|❌|
|`SetTableComment`|❌|❌|
|`SetViewComment`|❌|❌|
|`SetColumnComment`|❌|❌|
|`ShowColumns`|❌|❌|
|`AddColumn`|❌|❌|
|`AlterColumn`|❌|❌|
|`DropColumn`|❌|❌|
|`RenameColumn`|❌|❌|
|`InsertIntoTable`|❌|❌|
|`DeleteFromTable`|❌|❌|
|`TruncateTable`|❌|❌|
|`SelectFromColumns`|❌|✅|
|`UpdateTableColumns`|❌|✅|
|`CreateView`|❌|❌|
|`DropView`|❌|❌|
|`CreateViewWithSelectFromColumns`|❌|✅|
|`CreateMaterializedView`|✅|❌|
|`RefreshMaterializedView`|❌|❌|
|`SetMaterializedViewProperties`|✅|❌|
|`DropMaterializedView`|❌|❌|


Fields:

|Field name|Always set|Description|
|:-:|:-:|-|
|`operation`|✅|Operation being performed|
|`resource.table`|✅|Table information ([definition here](./trino-table.md))|

#### Example - table creation (with properties):

```json5
{
    "operation": "CreateTable",
    "resource": {
        "schema": {
            "catalogName": "some_catalog",
            "schemaName": "some_schema",
            "tableName": "some_table",
            "properties": {
                "schema_property": "table_property_value"
            }
        }
    }
}
```

#### Example - selecting from a table (with columns):

```json5
{
    "operation": "SelectFromColumns",
    "resource": {
        "schema": {
            "catalogName": "some_catalog",
            "schemaName": "some_schema",
            "tableName": "some_table",
            "columns": ["column_one", "column_two"]
        }
    }
}
```

> In this operation, the result should be `true` _only if_ the user can select from _both_ columns involved

#### Example - showing columns (neither columns nor properties):

```json5
{
    "operation": "ShowColumns",
    "resource": {
        "schema": {
            "catalogName": "some_catalog",
            "schemaName": "some_schema",
            "tableName": "some_table"
        }
    }
}
```

### `function` operations:

These operations involve functions or procedures, as defined by the [`function` object](./trino-function.md).

Operations:
- `ExecuteProcedure`
- `ExecuteFunction`
- `CreateViewWithExecuteFunction`
- `CreateFunction`
- `DropFunction`

#### Example:

```json5
{
    "operation": "SomeOperation",  // e.g. ExecuteProcedure
    "resource": {
        "function": {
            "catalogName": "some_catalog",
            "schema_name": "some_schema",
            "functionName": "some_function"
        }
    }
}
```

### Table procedure operation: `function` executed on a `table`

Operations:
- `ExecuteTableProcedure`

`ExecuteTableProcedure` executes a named table procedure on a specific table.

As such, it has two `resource` items:
- A [`table` object](./trino-table.md), denoting what table the procedure is operating on
- A [`function` object](./trino-function.md), denoting what function is being ran
    - This `function` object only contains a `functionName`

#### Example:
```json5
{
    "operation": "ExecuteTableProcedure",
    "resource": {
        "function": {
            "functionName": "some_procedure"
        },
        "table": {
            "catalogName": "some_catalog",
            "schemaName": "some_schema",
            "tableName": "some_table"
        }
    }
}
```

### Rename operations

Rename operations contain 2 fields:
- `resource`: denoting what resource is being renamed
- `targetResource`: denoting the new definition of the resource (i.e. new name)

|Operation|`resource` and `targetResource` type|
|-|-|
|`RenameSchema`|`schema` ([docs](./trino-schema.md))|
|`RenameTable`|`table` ([docs](./trino-table.md))|
|`RenameView`|`table` ([docs](./trino-table.md))|
|`RenameMaterializedView`|`table` ([docs](./trino-table.md))|

#### Example: renaming a table

```json5
{
    "operation": "RenameTable",
    "resource": {
        "catalogName": "some_catalog",
        "schemaName": "some_schema",
        "tableName": "some_table"
    },
    "targetResource": {
        "catalogName": "some_catalog",
        "schemaName": "some_schema",
        "tableName": "another_name"
    }
}
```

### Filtering operations

Filtering operations are used by Trino in order to filter a list of resources (e.g. catalogs, schemas, tables, ...)
and remove any items the current user should not be entitled to discover.

The specific format of the requests sent to OPA by these operations will depend on whether batch mode is enabled, refer to the plugin's [README](../README.md) file for more information on how to configure this.

Regardless of what mode is enabled, requests will always contain a `context` object as defined in "[The `context` object](#the-context-object)"

List of filtering operations:
- `FilterFunctions`
- `FilterColumns`
- `FilterViewQueryOwnedBy`
- `FilterCatalogs`
- `FilterSchemas`
- `FilterTables`

#### Filtering - with batch mode

See [filtering-batch.md](./filtering-batch.md) for more information

#### Filtering - no batch mode

See [filtering-no-batch.md](./filtering-no-batch.md) for more information

### Authorization & privileging operations

Using the SQL authorization and roles system together with OPA is an advanced setup, but it may be
useful in order to define broad rules as to what users can perform certain administrative operations; potentially
in conjunction with connector level access control rules.

The following operations are outlined in [sql-authorization.md](./sql-authorization.md):
- `SetSchemaAuthorization`
- `SetTableAuthorization`
- `SetViewAuthorization`
- `SetCatalogSessionProperty`
- `GrantSchemaPrivilege`
- `DenySchemaPrivilege`
- `RevokeSchemaPrivilege`
- `GrantTablePrivilege`
- `DenyTablePrivilege`
- `RevokeTablePrivilege`
- `CreateRole`
- `DropRole`
- `GrantRoles`
- `RevokeRoles`

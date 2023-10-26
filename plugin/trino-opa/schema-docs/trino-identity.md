# Trino identity object

This object contains information about the identity of a Trino user.

## Fields

It always contains the following 3 fields:

|Field name|Always set|Type|Description|
|:-:|:-:|:-:|-|
|`user`|✅|`string`|Name of the user|
|`groups`|✅|`Array[string]`|List of all groups this user belongs to (potentially empty)|
|`extraCredentials`|✅|`Map[string, string]`|Map of all additional credentials and their values (potentially empty)|

## Example

```json5
{
    "user": "username",
    "groups": ["group-1", "group-2"],
    "extraCredentials": {
        "some-credential": "some-credential-value"
    }
}
```

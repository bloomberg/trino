/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.opa.schema;

import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.type.Type;
import jakarta.validation.constraints.NotNull;

import static java.util.Objects.requireNonNull;

public record TrinoColumn(
        @NotNull String catalogName,
        @NotNull String schemaName,
        @NotNull String tableName,
        @NotNull String columnName,
        @NotNull String columnType)
{
    public TrinoColumn
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(columnName, "columnName is null");
        requireNonNull(columnType, "columnType is null");
    }

    public TrinoColumn(CatalogSchemaTableName tableName, String columnName, Type type)
    {
        this(tableName.getCatalogName(),
                tableName.getSchemaTableName().getSchemaName(),
                tableName.getSchemaTableName().getTableName(),
                columnName,
                type.getDisplayName());
    }
}

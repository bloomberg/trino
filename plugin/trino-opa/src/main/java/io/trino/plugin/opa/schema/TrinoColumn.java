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

import io.trino.spi.connector.SchemaTableName;

import static java.util.Objects.requireNonNull;

/**
 * Helper record used to denote a specific column within a schema & table pair
 * This is only used in filterColumns where simpler objects than TrinoTable help
 * keep the code cleaner
 *
 * @param schemaTableName Schema and table name
 * @param columnName Column name
 */
public record TrinoColumn(SchemaTableName schemaTableName, String columnName)
{
    public TrinoColumn
    {
        requireNonNull(schemaTableName, "schemaTableName is null");
        requireNonNull(columnName, "columnName is null");
    }
}

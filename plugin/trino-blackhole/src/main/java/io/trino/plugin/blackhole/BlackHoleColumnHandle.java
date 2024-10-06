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
package io.trino.plugin.blackhole;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import static java.util.Objects.requireNonNull;

public record BlackHoleColumnHandle(String name, Type columnType)
        implements ColumnHandle
{
    public BlackHoleColumnHandle
    {
        requireNonNull(name, "name is null");
        requireNonNull(columnType, "columnType is null");
    }

    @JsonIgnore
    public ColumnMetadata toColumnMetadata()
    {
        return new ColumnMetadata(name, columnType);
    }

    @Override
    public String toString()
    {
        return name + ":" + columnType;
    }
}

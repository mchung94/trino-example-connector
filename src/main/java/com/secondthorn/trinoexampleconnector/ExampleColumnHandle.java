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
package com.secondthorn.trinoexampleconnector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import java.util.Objects;

public class ExampleColumnHandle
        implements ColumnHandle
{
    private final String columnName;
    private final Type columnType;

    public ExampleColumnHandle(ColumnMetadata columnMetadata)
    {
        this(columnMetadata.getName(), columnMetadata.getType());
    }

    @JsonCreator
    public ExampleColumnHandle(String columnName, Type columnType)
    {
        this.columnName = columnName;
        this.columnType = columnType;
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public Type getColumnType()
    {
        return columnType;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(columnName, columnType);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ExampleColumnHandle that = (ExampleColumnHandle) o;
        return Objects.equals(columnName, that.columnName) && Objects.equals(columnType, that.columnType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName, columnType);
    }
}

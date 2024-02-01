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

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class ExampleMetadata
        implements ConnectorMetadata
{
    private final DataSource dataSource;

    public ExampleMetadata(DataSource dataSource)
    {
        this.dataSource = dataSource;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return List.of(dataSource.getSchemaTableName().getSchemaName());
    }

    @jakarta.annotation.Nullable
    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session,
                                               SchemaTableName tableName,
                                               Optional<ConnectorTableVersion> startVersion,
                                               Optional<ConnectorTableVersion> endVersion)
    {
        if (tableName.equals(dataSource.getSchemaTableName())) {
            return new ExampleTableHandle(tableName);
        }
        return null;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        final ExampleTableHandle exampleTableHandle = (ExampleTableHandle) table;
        final SchemaTableName schemaTableName = exampleTableHandle.getSchemaTableName();
        if (schemaTableName.equals(dataSource.getSchemaTableName())) {
            return new ConnectorTableMetadata(schemaTableName, dataSource.getColumnMetadata());
        }
        return null;
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        final SchemaTableName schemaTableName = dataSource.getSchemaTableName();
        return schemaName.filter(schemaTableName.getSchemaName()::equals)
                .map(s -> schemaTableName)
                .stream().toList();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        final ExampleTableHandle exampleTableHandle = (ExampleTableHandle) tableHandle;
        final SchemaTableName schemaTableName = exampleTableHandle.getSchemaTableName();
        if (schemaTableName.equals(dataSource.getSchemaTableName())) {
            return dataSource.getColumnMetadata().stream()
                    .collect(Collectors.toMap(ColumnMetadata::getName, ExampleColumnHandle::new));
        }
        else {
            throw new TableNotFoundException(schemaTableName);
        }
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        final ExampleTableHandle exampleTableHandle = (ExampleTableHandle) tableHandle;
        final SchemaTableName schemaTableName = exampleTableHandle.getSchemaTableName();
        if (schemaTableName.equals(dataSource.getSchemaTableName())) {
            return ((ExampleColumnHandle) columnHandle).getColumnMetadata();
        }
        return null;
    }
}

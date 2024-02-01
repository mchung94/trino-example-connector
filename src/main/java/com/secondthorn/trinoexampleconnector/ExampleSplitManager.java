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

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.SchemaTableName;

public class ExampleSplitManager
        implements ConnectorSplitManager
{
    private final DataSource dataSource;

    public ExampleSplitManager(DataSource dataSource)
    {
        this.dataSource = dataSource;
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction,
                                          ConnectorSession session,
                                          ConnectorTableHandle table,
                                          DynamicFilter dynamicFilter,
                                          Constraint constraint)
    {
        // return a single hardcoded junk split for the entire table
        final ExampleTableHandle exampleTableHandle = (ExampleTableHandle) table;
        final SchemaTableName schemaTableName = exampleTableHandle.getSchemaTableName();
        if (schemaTableName.equals(dataSource.getSchemaTableName())) {
            ConnectorSplit split = new ExampleSplit("all");
            return new FixedSplitSource(split);
        }
        return null;
    }
}

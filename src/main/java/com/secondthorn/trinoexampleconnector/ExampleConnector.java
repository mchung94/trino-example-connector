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

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;

import static com.secondthorn.trinoexampleconnector.ExampleTransactionHandle.INSTANCE;

public class ExampleConnector
        implements Connector
{
    private final DataSource dataSource = new DataSource();
    private final ExampleMetadata exampleMetadata = new ExampleMetadata(dataSource);
    private final ExampleSplitManager exampleSplitManager = new ExampleSplitManager(dataSource);
    private final ExamplePageSourceProvider examplePageSourceProvider = new ExamplePageSourceProvider(dataSource);

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        return INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        return exampleMetadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return exampleSplitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return examplePageSourceProvider;
    }
}

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

import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;

import java.util.Collections;
import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;

public class ExampleQueryRunner
{
    private ExampleQueryRunner()
    {
    }

    static DistributedQueryRunner createDistributedQueryRunner()
            throws Exception
    {
        return makeDistributedQueryRunner(Collections.emptyMap());
    }

    static DistributedQueryRunner createDistributedQueryRunner(int port)
            throws Exception
    {
        return makeDistributedQueryRunner(Map.of("http-server.http.port", String.valueOf(port)));
    }

    private static DistributedQueryRunner makeDistributedQueryRunner(Map<String, String> extraProperties)
            throws Exception
    {
        Session defaultSession = testSessionBuilder().build();
        DistributedQueryRunner runner = DistributedQueryRunner.builder(defaultSession)
                .setExtraProperties(extraProperties)
                .setNodeCount(1)
                .build();
        runner.installPlugin(new ExamplePlugin());
        Map<String, String> connectorProperties = Collections.emptyMap();
        runner.createCatalog("example", "example_connector", connectorProperties);
        return runner;
    }

    public static void main(String[] args)
            throws Exception
    {
        DistributedQueryRunner runner = createDistributedQueryRunner(8080); // default port for Trino CLI
        Logging.initialize();
        Logger log = Logger.get(ExampleQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("==== %s ====", runner.getCoordinator().getBaseUrl());
        // You can run queries using runner.execute() here if you want...
        // Don't close the runner here, it should keep running after this point
        // so that it stays up for Trino CLI to talk to it.
    }
}

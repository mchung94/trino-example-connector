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

import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedRow;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExampleQueryTest
{
    private static DistributedQueryRunner runner;

    @BeforeAll
    public static void initializeQueryRunner()
    {
        try {
            runner = ExampleQueryRunner.createDistributedQueryRunner();
        }
        catch (Exception ex) {
            Assertions.fail("Failing all query tests because we can't initialize the query runner.", ex);
        }
    }

    @AfterAll
    public static void closeQueryRunner()
    {
        runner.close();
    }

    private List<MaterializedRow> execute(String sql)
    {
        return runner.execute(sql).getMaterializedRows();
    }

    @Test
    public void showCatalogsHasExampleCatalog()
    {
        List<MaterializedRow> rows = execute("SHOW CATALOGS");
        assertTrue(rows.stream().anyMatch(row -> row.getField(0).equals("example")));
    }

    @Test
    public void showSchemasHasMySchema()
    {
        List<MaterializedRow> rows = execute("SHOW SCHEMAS FROM example");
        assertTrue(rows.stream().anyMatch(row -> row.getField(0).equals("my_schema")));
    }

    @Test
    public void showTablesFromMySchemaHasMyTable()
    {
        List<MaterializedRow> rows = execute("SHOW TABLES FROM example.my_schema");
        assertTrue(rows.stream().anyMatch(row -> row.getField(0).equals("my_table")));
    }

    @Test
    public void showColumnsHasColumns()
    {
        List<MaterializedRow> rows = execute("SHOW COLUMNS FROM example.my_schema.my_table");
        assertEquals(5, rows.size());
        assertTrue(rows.stream().anyMatch(row -> row.getField(0).equals("name")));
        assertTrue(rows.stream().anyMatch(row -> row.getField(0).equals("measurement_time")));
        assertTrue(rows.stream().anyMatch(row -> row.getField(0).equals("temperature")));
        assertTrue(rows.stream().anyMatch(row -> row.getField(0).equals("group_size")));
        assertTrue(rows.stream().anyMatch(row -> row.getField(0).equals("is_raining")));
    }

    @Test
    public void selectAllFromTable()
    {
        List<MaterializedRow> rows = execute("SELECT * FROM example.my_schema.my_table");
        assertEquals(8, rows.size());
    }

    @Test
    public void selectGroupBy()
    {
        List<MaterializedRow> rows = execute("select name, " +
                "is_raining, " +
                "min(measurement_time) as min_measurement_time, " +
                "max(measurement_time) as max_measurement_time, " +
                "avg(temperature) as avg_temperature, " +
                "sum(group_size) as sum_group_size " +
                "from example.my_schema.my_table " +
                "where is_raining=true " +
                "group by name, is_raining");
        assertEquals(1, rows.size());
    }
}

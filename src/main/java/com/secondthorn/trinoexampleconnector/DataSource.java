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

import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class DataSource
{
    public record Row(String name, Instant measurementTime, Double temperature, Integer groupSize, Boolean isRaining) {}

    private static final SchemaTableName SCHEMA_TABLE_NAME = new SchemaTableName("my_schema", "my_table");

    private static final List<Row> TABLE = List.of(
            new Row("foo", Instant.ofEpochMilli(1704145507213L), 23.43, 123, true),
            new Row("foo", Instant.ofEpochMilli(1704145507563L), 24.43, 1234, true),
            new Row("foo", Instant.ofEpochMilli(1704145507913L), 25.43, 12345, true),
            new Row("foo", Instant.ofEpochMilli(1704145508263L), 26.43, 123456, true),
            new Row("bar", Instant.ofEpochMilli(1704145507129L), 23.43, 123, false),
            new Row("bar", Instant.ofEpochMilli(1704145507479L), 22.43, 1234, false),
            new Row("bar", Instant.ofEpochMilli(1704145507829L), 21.43, 12345, false),
            new Row("bar", Instant.ofEpochMilli(1704145508179L), 20.43, 123456, false));

    private static final Map<Class, Type> JAVA_TO_TRINO_TYPE = Map.of(
            java.lang.String.class, VarcharType.VARCHAR,
            java.time.Instant.class, TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS,
            java.lang.Double.class, DoubleType.DOUBLE,
            java.lang.Integer.class, IntegerType.INTEGER,
            java.lang.Boolean.class, BooleanType.BOOLEAN);

    private static final Map<String, String> JAVA_COLUMN_TO_TRINO_COLUMN_NAME = Map.of(
            "name", "name",
            "measurementTime", "measurement_time",
            "temperature", "temperature",
            "groupSize", "group_size",
            "isRaining", "is_raining");

    public SchemaTableName getSchemaTableName()
    {
        return SCHEMA_TABLE_NAME;
    }

    public List<ColumnMetadata> getColumnMetadata()
    {
        return Arrays.stream(Row.class.getRecordComponents())
                .map(r -> new ColumnMetadata(
                        JAVA_COLUMN_TO_TRINO_COLUMN_NAME.get(r.getName()),
                        JAVA_TO_TRINO_TYPE.get(r.getType())))
                .toList();
    }

    public List<Row> getTableRows()
    {
        return TABLE;
    }
}

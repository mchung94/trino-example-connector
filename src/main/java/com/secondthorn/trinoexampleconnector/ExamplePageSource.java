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

import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;

public class ExamplePageSource
        implements ConnectorPageSource
{
    private final DataSource dataSource;
    private final ExampleSplit split;
    private final ExampleTableHandle tableHandle;
    private final List<ColumnHandle> columns;
    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final PageBuilder pageBuilder;
    private boolean isFinished;

    public ExamplePageSource(DataSource dataSource,
                             ExampleSplit split,
                             ExampleTableHandle tableHandle,
                             List<ColumnHandle> columns)
    {
        this.dataSource = dataSource;
        this.split = split;
        this.tableHandle = tableHandle;
        this.columns = columns;
        this.columnNames = columns.stream().map(c -> ((ExampleColumnHandle) c).getColumnName()).toList();
        this.columnTypes = columns.stream().map(c -> ((ExampleColumnHandle) c).getColumnType()).toList();
        this.pageBuilder = new PageBuilder(this.columnTypes);
        this.isFinished = false;
    }

    @Override
    public boolean isFinished()
    {
        return isFinished;
    }

    @Override
    public Page getNextPage()
    {
        // return the entire table in one Page
        for (DataSource.Row row : dataSource.getTableRows()) {
            this.pageBuilder.declarePosition();
            for (int column = 0; column < this.columns.size(); column++) {
                String columnName = this.columnNames.get(column);
                Type columnType = this.columnTypes.get(column);
                BlockBuilder blockBuilder = this.pageBuilder.getBlockBuilder(column);
                switch (columnName) {
                    case "name":
                        String name = row.name();
                        if (name == null) {
                            blockBuilder.appendNull();
                        }
                        else {
                            columnType.writeSlice(blockBuilder, utf8Slice(name));
                        }
                        break;
                    case "measurement_time":
                        Instant measurementTime = row.measurementTime();
                        if (measurementTime == null) {
                            blockBuilder.appendNull();
                        }
                        else {
                            LongTimestampWithTimeZone ts = LongTimestampWithTimeZone.fromEpochSecondsAndFraction(
                                    measurementTime.getEpochSecond(),
                                    ((long) measurementTime.getNano()) * PICOSECONDS_PER_NANOSECOND,
                                    TimeZoneKey.UTC_KEY);
                            columnType.writeObject(blockBuilder, ts);
                        }
                        break;
                    case "temperature":
                        Double temperature = row.temperature();
                        if (temperature == null) {
                            blockBuilder.appendNull();
                        }
                        else {
                            columnType.writeDouble(blockBuilder, temperature);
                        }
                        break;
                    case "group_size":
                        Integer groupSize = row.groupSize();
                        if (groupSize == null) {
                            blockBuilder.appendNull();
                        }
                        else {
                            columnType.writeLong(blockBuilder, groupSize);
                        }
                        break;
                    case "is_raining":
                        Boolean isRaining = row.isRaining();
                        if (isRaining == null) {
                            blockBuilder.appendNull();
                        }
                        else {
                            columnType.writeBoolean(blockBuilder, isRaining);
                        }
                        break;
                }
            }
        }
        Page page = pageBuilder.build();
        isFinished = true;
        return page;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
            throws IOException
    {
    }
}

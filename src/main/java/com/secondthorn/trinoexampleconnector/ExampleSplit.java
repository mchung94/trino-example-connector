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
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.Collections;
import java.util.List;

public class ExampleSplit
        implements ConnectorSplit
{
    private final String unimportantField;

    @JsonCreator
    public ExampleSplit(String unimportantField)
    {
        this.unimportantField = unimportantField;
    }

    @JsonProperty
    public String getUnimportantField()
    {
        return unimportantField;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return Collections.emptyList();
    }

    @Override
    public Object getInfo()
    {
        return this;
    }
}

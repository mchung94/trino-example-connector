# trino-example-connector
This is a small example of a Trino connector plugin. Connectors make your data
look like tables to Trino, so that you can run SQL queries on your data.

# Description
This is the example I wish I had when I was starting to learn about Trino:
- It has as few dependencies as possible. I want my example to be more clear
  about what's necessary for a functioning Trino connector plugin.
- It does not disable any Maven plugins or checks that normally run during the
  build process, so it'll follow the same standard as the official Trino
  plugins.
- It is a standalone Maven project with `<packaging>trino-plugin</packaging>`
  so that it builds a .zip file that you can unzip in a Trino deployment's
  plugin directory where all the other plugins are.
- For development, there is a query runner class in the test code that will run
  the connector and listen to port 8080, and then you can use the Trino CLI to
  send queries to it.

# Limitations
This is an over-simplified example that exists to help you get started.

Some of the limitations of this example are:
- The data source is hardcoded and read-only.
- There is only one schema, and only one table in the schema.
- There is no dependency injection. Look at other plugins for examples.
- There are no configuration options for the connector. The Trino documentation
  has information on how to do this.
- The next step would be to use the ConnectorSplitManager to partition the data
  into pieces that can be processed in parallel. In this example, the connector
  just returns a single split for the entire table.

# Alternative Examples and Documentation
I wanted this example to be a first example, but there's a lot of good
information available:
- Trino has excellent [docs](https://trino.io/docs/current/develop.html)
on developing plugins and has some example connectors you can look at.
- You can also see the source of the official [plugins](https://github.com/trinodb/trino/tree/master/plugin)
  that come with Trino.
- I think [trino-plugin-archetype](https://github.com/nineinchnick/trino-plugin-archetype)
  is the right place to go when you want to start creating your own plugin.
  - My example has the parent POM as `trino-root` which lets my pom.xml have
    less stuff defined in it, and also allows the packaging to be the
    `trino-plugin` .zip file. But doing this ties the plugin to a specific
    Trino release, which might not be what you want.

# Building the Connector
This is just a standard Maven project, so you can just run something like
`./mvnw clean verify` to do a full build from scratch.

# Deploying the Connector
When you build this plugin, use the .zip file it generates and unzip it in the
Trino deployment's plugin directory where all the other plugins are at. Then
set up the catalog properties file for the plugin.

# Developing the Connector
First, run `ExampleQueryRunner` in the test code. It'll listen on localhost
port 8080.

Then you can run the [Trino CLI](https://trino.io/docs/current/client/cli.html)
against it and start running queries.

Also, the `ExampleQueryTest` class has examples of using the
`ExampleQueryRunner` in JUnit tests.

# Sample Queries
```
show catalogs;

show schemas from trivial;

show tables from trivial.my_schema;

show columns from trivial.my_schema.my_table;

select * from trivial.my_schema.my_table;

select name,
       is_raining,
       min(measurement_time) as min_measurement_time,
       max(measurement_time) as max_measurement_time,
       avg(temperature) as avg_temperature,
       sum(group_size) as sum_group_size
from example.my_schema.my_table
where is_raining=true
group by name, is_raining;
```

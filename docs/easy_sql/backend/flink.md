## Flink backend

For flink backend, both `batch` and `streaming` mode are supported.

To use flink as backend, there are a few things we need to know.

### ETL mode

Both `batch` and `streaming` mode are supported in flink backend. But we still need to specify one before running the ETL.

To specify which mode to use in the ETL, we could add a configuration as below:

``` SQL
-- The value of the configuration below could be 'batch' or 'streaming'.
-- config: easy_sql.etl_type=streaming
```

### Connector configuration

A flink application reads data from some data source and write data to some other system. Both the source and target are configured by connectors. (Refer [here](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/table/overview/) to know about it.)

To write a flink application in Easy SQL, we need to provide a configuration file to describe the connectors used.

The configuration file is in JSON format and the structure of it is intuitive. Below is a sample configuration with detailed description. (Please read the comment to understand the configuration design.)

```json
{
    // Define catalogs to use in flink to find tables
    "catalogs": [
        {
            // Name is used as the catalog name. The other properties are used to create the catalog.
            // More at https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/table/hive/hive_catalog/
            "name": "myHiveCatalog",
            "type": "hive",
            "hive-conf-dir": "path/to/hive_conf"
        },
        {
            // Name is used as the catalog name. The other properties are used to create the catalog.
            // More at https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/table/jdbc/#usage-of-jdbc-catalog
            "name": "myJdbcCatalog",
            "type" = "jdbc",
            "default-database" = "...",
            "username" = "...",
            "password" = "...",
            "base-url" = "..."
        }
    ],
    // Define databases and tables that will be regsitered in flink.
    // The below configuration could be generated from the data source.
    "databases": [
        {
            // The database name to register in flink
            "name": "db_1",
            // The connectors to use to register table in flink. Connectors could be reused across tables.
            "connectors": [
                {
                    // Name of the connector, should be unique in one database
                    "name": "connector_1",
                    // Options of the connector, refer to the corresponding connector for more information.
                    // More at https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/table/overview/.
                    // When used to register table, the options here will be updated by options defined in table section.
                    "options": {
                        "connector": "jdbc",
                        "url": "jdbc:postgresql://localhost:5432/postgres",
                        "username": "postgres",
                        "password": "postgres"
                    }
                },
                {
                    "name": "connector_2",
                    "options": {
                        "connector": "postgres-cdc",
                        "hostname": "localhost",
                        "port": "5432",
                        "database-name": "postgres",
                        "schema-name": "postgres",
                        "username": "postgres",
                        "password": "postgres"
                    }
                }
            ],
            // Defined tables that will be registered under the database.
            "tables": [
                {
                    // Name of the table
                    "name": "source_1",
                    // Connector of the table
                    "connector": {
                        "name": "connector_1",
                        // Connector options, will be merged with connector options above and take precedence.
                        "options": {
                            "table-name": "sample.test"
                        }
                    },
                    // Schema definition of the table, used to create table
                    "schema": [
                        "`id` INT",
                        "val VARCHAR",
                        "PRIMARY KEY (id) NOT ENFORCED"
                    ],
                    // Partition definition of the table, used to create table
                    "partition_by": ["id", "val"]
                },
                {
                    "name": "target_1",
                    "connector": {
                        "name": "connector_1",
                        "options": {
                            "table-name": "out_put_table"
                        }
                    },
                    "schema": [
                        "id INT",
                        "val VARCHAR",
                        "PRIMARY KEY (id) NOT ENFORCED"
                    ]
                }
            ]
        },
        {
            "name": "db_2",
            "connectors": [
            ],
            "tables": [
            ]
        }
    ]
}
```

### Register catalogs and tables

To specify which configuration file to use in an ETL. We could use a configuration directive. An example is as below:

``` SQL
-- config: easy_sql.flink_tables_file_path=path/to/your/connectors_config.json
```

After we specified this configuration, Easy SQL will try register the catalogs defined in it.

If we'd like to register tables in flink, we need to use the `inputs`/`outputs` directive, which states tables that are used as inputs or outputs in this ETL. An example could be:

``` SQL
-- inputs: db_1.source_1, db_1.target_1
-- outputs: db_1.target_1
```

Easy SQL will only register tables if they are stated as inputs or outputs of the ETL.

### `prepare-sql` directive

Before we start, please be noted that the `prepare-sql` directive usually should be used only in test cases.

If we'd like to use `prepare-sql` in our ETL. Easy SQL will execute sql in and potentially write data to some connector.

For simplicity, Easy SQL chooses the flink connector of the first table stated in the ETL with `inputs` and `outputs` directives. For the example below, the selected connector is the connector of table `db_1.source_1`.

``` SQL
-- inputs: db_1.source_1, db_1.target_1
-- outputs: db_1.target_1
```

### **Cannot** create output table automatically

Since not all the connectors of flink have support for DDL operations, there are limitations for us to create output table automatically.

As of flink 1.15, the hive connector implemented the DDL API and could be used to create output table. (Refer [here](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/table/hive/hive_read_write/#writing).) The support could be added in the near future.

In this case,

- `__create_output_table__` variable is ignored.
- partitions defined by `__partition__` will only be used to create an extra column before saving to the target system.

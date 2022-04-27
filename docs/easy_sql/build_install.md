# Build and install Easy SQL

Easy SQL is a very light-weight python library. The common Python library conventions are followed.
It's easy to build or install Easy SQL.

## Install Easy SQL

Install Easy SQL using pip: `python3 -m pip install easy_sql-easy_sql`

## Building Easy SQL

Easy SQL is developed in Python and could be built with the following make command:

```bash
make package-pip
```

After the above command, there will be a file named `easy_sql*.whl` generated in the `dist` folder.
You can install it with command `python3 -m pip install dist/easy_sql*.whl`.

## Dependencies

Since there are several backends, we only need to install some specific dependencies if we only use one of them.

For spark, you need to install some version of `pyspark`.

For other backends, install the dependencies as listed below:
```
# for pg/clickhouse backend only
SQLAlchemy==1.3.23
# for pg backend only
psycopg2-binary==2.8.6
# for clickhouse backend only
clickhouse-driver==0.2.0
clickhouse-sqlalchemy==0.1.6
# for BigQuery backend only
sqlalchemy-bigquery==1.4.3
# for MaxCompute backend only
pyodps==0.10.7.1
```

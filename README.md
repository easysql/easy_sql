# Easy SQL

Easy SQL is built to ease the data ETL development process.
With Easy SQL, you can develop your ETL in SQL in an imperative way.
It defines a few simple syntax on top of standard SQL, with which SQL could be executed one by one.
Easy SQL also provides a processor to handle all the new syntax.
Since this is SQL agnostic, any SQL engine could be plugged-in as a backend.
There are built-in support for several popular SQL engines, including SparkSQL, PostgreSQL, Clickhouse, Aliyun Maxcompute, Google BigQuery.
More will be added in the near future.

- Docs: <https://easy-sql.readthedocs.io/>
- Enterprise extended product: <https://data-workbench.com/>

[![GitHub Action Build](https://github.com/easysql/easy_sql/actions/workflows/build.yaml/badge.svg?branch=main&event=push)](https://github.com/easysql/easy_sql/actions/workflows/build.yaml?query=branch%3Amain+event%3Apush)
[![Docs Build](https://readthedocs.org/projects/easy-sql/badge/?version=latest)](https://easy-sql.readthedocs.io/en/latest/?badge=latest)
[![EasySQL Coverage](https://codecov.io/gh/easysql/easy_sql/branch/main/graph/badge.svg)](https://codecov.io/gh/easysql/easy_sql)

## Install Easy SQL

Install Easy SQL using pip: `python3 -m pip install easy_sql-easy_sql`

## Building Easy SQL

Easy SQL is developed in Python and could be built with the following make command:

```bash
make package-pip
```

After the above command, there will be a file named `easy_sql*.whl` generated in the `dist` folder.
You can install it with command `pip install dist/easy_sql*.whl`.

## Dependencies

Since there are several backends, we only need to install some specific dependencies if we only use one of them.

For spark, you need to install some version of `pyspark`.

For other backends, install the dependencies as listed below:
```
# for pg/clickhouse/bigquery backend only
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

To use other tools / functionalities, install the dependencies as listed below:

```
# for Linter only
sqlfluff==1.2.1
colorlog==4.0.2
regex==2022.6.2
# for command line tools only
click==6.7
```

## First ETL with Easy SQL

(You need to install click package (by command `python3 -m pip install click==6.7`) before run the command below.)

### For spark backend

Create a file named `sample_etl.spark.sql` with content as below:

```sql
-- prepare-sql: drop database if exists sample cascade
-- prepare-sql: create database sample
-- prepare-sql: create table sample.test as select 1 as id, '1' as val

-- target=variables
select true as __create_output_table__

-- target=variables
select 1 as a

-- target=log.a
select '${a}' as a

-- target=log.test_log
select 1 as some_log

-- target=check.should_equal
select 1 as actual, 1 as expected

-- target=temp.result
select
    ${a} as id, ${a} + 1 as val
union all
select id, val from sample.test

-- target=output.sample.result
select * from result

-- target=log.sample_result
select * from sample.result
```

Run it with command:

```bash
bash -c "$(python3 -m easy_sql.data_process -f sample_etl.spark.sql -p)"
```

### For postgres backend:

You need to start a postgres instance first.

If you have docker, run the command below:

```bash
docker run -d --name postgres -p 5432:5432 -e POSTGRES_PASSWORD=123456 postgres
```

Create a file named `sample_etl.postgres.sql` with content as the test file [here](https://github.com/easysql/easy_sql/blob/main/test/sample_etl.postgres.sql).

Run it with command:

```bash
PG_URL=postgresql://postgres:123456@localhost:5432/postgres python3 -m easy_sql.data_process -f sample_etl.postgres.sql
```

### For clickhouse backend:

You need to start a clickhouse instance first.

If you have docker, run the command below:

```bash
docker run -d --name clickhouse -p 9000:9000 yandex/clickhouse-server:20.12.5.18
```

Create a file named `sample_etl.clickhouse.sql` with content as the test file [here](https://github.com/easysql/easy_sql/blob/main/test/sample_etl.clickhouse.sql).

Run it with command:

```bash
CLICKHOUSE_URL=clickhouse+native://default@localhost:9000 python3 -m easy_sql.data_process -f sample_etl.clickhouse.sql
```

### For other backends:

The usage is similar, please refer to API.

## Run ETL in your code

Easy SQL can be used as a very light-weight library. If you'd like to run ETL programmatically in your code.
Please refer to the code snippets below:

```python
from pyspark.sql import SparkSession

from easy_sql.sql_processor import SqlProcessor
from easy_sql.sql_processor.backend import SparkBackend

if __name__ == '__main__':
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    backend = SparkBackend(spark)
    sql = '''
-- target=log.some_log
select 1 as a
    '''
    sql_processor = SqlProcessor(backend, sql)
    sql_processor.run()
```

More sample code about other backends could be referred [here](https://github.com/easysql/easy_sql/blob/main/test/sample_data_process.py)

## Debugging ETL

We recommend debugging ETLs from jupyter. You can follow the steps below to start debugging your ETL.

1. Install jupyter first with command `pip install jupyterlab`.

2. Create a file named `debugger.py` with contents like below:

A more detailed sample could be found [here](https://github.com/easysql/easy_sql/blob/main/debugger.py).

```python
from typing import Dict, Any

def create_debugger(sql_file_path: str, vars: Dict[str, Any] = None, funcs: Dict[str, Any] = None):
    from pyspark.sql import SparkSession
    from easy_sql.sql_processor.backend import SparkBackend
    from easy_sql.sql_processor_debugger import SqlProcessorDebugger
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    backend = SparkBackend(spark)
    debugger = SqlProcessorDebugger(sql_file_path, backend, vars, funcs)
    return debugger

```

3. Create a file named `test.sql` with contents as [here](https://github.com/easysql/easy_sql/blob/main/test/sample_etl.spark.sql).

4. Then start jupyter lab with command: `jupyter lab`.

5. Start debugging like below:

![ETL Debugging](https://raw.githubusercontent.com/easysql/easy_sql/main/debugger-usage.gif)

## Contributing

Please submit PR.

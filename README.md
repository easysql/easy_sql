# Easy SQL

Easy SQL is built to ease the data ETL development process.
With Easy SQL, you can develop your ETL in SQL in an imperative way.
It defines a few simple syntax on top of standard SQL, with which SQL could be executed one by one.
Easy SQL also provides a processor to handle all the new syntax.
Since this is SQL agnostic, any SQL engine could be plugged-in as a backend.
There are built-in support for several popular SQL engines, including SparkSQL, PostgreSQL, Clickhouse, FlinkSQL, Aliyun Maxcompute, Google BigQuery.
More will be added in the near future.

- Docs: <https://easy-sql.readthedocs.io/>
- Enterprise extended product: <https://data-workbench.com/>

[![GitHub Action Build](https://github.com/easysql/easy_sql/actions/workflows/build.yaml/badge.svg?branch=main&event=push)](https://github.com/easysql/easy_sql/actions/workflows/build.yaml?query=branch%3Amain+event%3Apush)
[![Docs Build](https://readthedocs.org/projects/easy-sql/badge/?version=latest)](https://easy-sql.readthedocs.io/en/latest/?badge=latest)
[![EasySQL Coverage](https://codecov.io/gh/easysql/easy_sql/branch/main/graph/badge.svg)](https://codecov.io/gh/easysql/easy_sql)
[![PyPI](https://img.shields.io/pypi/v/easy-sql-easy-sql)](https://pypi.org/project/easy-sql-easy-sql/)

## Install Easy SQL

Install Easy SQL using pip: `python3 -m pip install easy_sql-easy_sql[extra,extra]`

Currently we are providing below extras, choose according to your need:
- cli
- linter
- spark
- pg
- clickhouse

We also provide flink backend, but because of dependency confliction between pyspark and apache-flink, you need to install the flink backend dependencies manually with the following command `python3 -m pip install apache-flink`.

Usually we read data from some data source and write data to some other system using flink with different connectors. So we need to download some jars for the used connectors as well. Refer [here](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/table/overview/) to get more information and [here](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/table/downloads/) to download the required connectors.

## Building Easy SQL

Internally we use `poetry` to manage the dependencies. So make sure you have [installed it](https://python-poetry.org/docs/master/#installation). Package could be built with the following make command: `make package-pip` or just `poetry build`.

After the above command, there will be a file named `easy_sql*.whl` generated in the `dist` folder.
You can install it with command `python3 -m pip install dist/easy_sql*.whl[extra]` or just `poetry install -E 'extra extra'`.

## First ETL with Easy SQL

Install easy_sql with spark as the backend: `python3 -m pip install easy_sql-easy_sql[spark,cli]`.

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

Make sure that you have install the corresponding backend with `python3 -m pip install easy-sql-easy-sql[cli,pg]`

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

Make sure that you have install the corresponding backend with `python3 -m pip install easy-sql-easy-sql[cli,clickhouse]`

Run it with command:

```bash
CLICKHOUSE_URL=clickhouse+native://default@localhost:9000 python3 -m easy_sql.data_process -f sample_etl.clickhouse.sql
```

### For flink backend:

Because of dependency conflictions between pyspark and apache-flink, you need to install flink manually with command `python3 -m pip install apache-flink`.

After the installation, you need to add flink commands directory to PATH environment variable to make flink commands discoverable by bash. To do it, execute the commands below:

```bash
export FLINK_HOME=$(python3 -m pyflink.find_flink_home)
export PATH=$FLINK_HOME/bin:$PATH
export PYFLINK_CLIENT_EXECUTABLE=python3  # Set Python interpreter for flink client.
```

You can add these commands to your `.bashrc` or `.zshrc` file for convenience.

Since there are many connectors for flink, you need to choose which connector to use before starting.

As an example, if you want to read or write data to postgres, then you need to start a postgres instance first.

If you have docker, run the command below:

```bash
docker run -d --name postgres -p 5432:5432 -e POSTGRES_PASSWORD=123456 postgres
```

Download the required jars as below:

```bash
mkdir -pv test/flink/jars
wget -P test/flink/jars https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/1.15.1/flink-connector-jdbc-1.15.1.jar
wget -P test/flink/jars https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.14/postgresql-42.2.14.jar
```

Create a file named `sample_etl.flink.postgres.sql` with content as the test file [here](https://github.com/easysql/easy_sql/blob/main/test/sample_etl.flink.postgres.sql).

Create a connector configuration file named `sample_etl.flink_tables_file.json` with content as the test configuration file [here](https://github.com/easysql/easy_sql/blob/main/test/sample_etl.flink_tables_file.json).

Run it with command:

```bash
bash -c "$(python3 -m easy_sql.data_process -f sample_etl.flink.postgres.sql -p)"
```

There are a few other things to know about flink, click [here](https://easy-sql.readthedocs.io/en/latest/easy_sql/backend/flink.html) to get more information.

### For other backends:

The usage is similar, please refer to API doc [here](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/backend/index.html).

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

1. Install jupyter first with command `python3 -m pip install jupyterlab`.

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

## ETL Language support

We've created an extension for VS Code to ease the development of ETL in Easy SQL. A bunch of language features are provided, e.g. syntax highlight, code completion, diagnostics features etc. You can search `Easy SQL` in extension marketplace, or click [here](https://marketplace.visualstudio.com/items?itemName=EasySQL.easysql&ssr=false#overview) to get more information.

We recommended to install the extension to develop ETL in Easy SQL.

## Contributing

Please submit PR.
